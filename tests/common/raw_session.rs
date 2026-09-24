//! The harness's own session client, speaking the FlatBuffers session protocol directly.
//!
//! Outside the layer order: a test harness. Product code must not name it.
//!
//! - **Owns.** One native gRPC exchange per test session: request identities, reply routing and
//!   transfer reassembly, the subscriptions the session opened and the display text of their rows,
//!   the notices it received, and raw frames a scenario sends to probe the server's refusals.
//! - **Depends on.** The client wire contract and its gRPC codec, the NSPL client statement parser
//!   to route subscription statements, and the shared TLS and credential fixtures.
//! - **Must not know.** Server internals; everything it observes arrives through the public
//!   protocol.
//!
//! The session reads frames only while a caller waits for something, so every reply and event
//! that arrives while it waits for another is kept until asked for.

use std::{
    collections::{BTreeMap, VecDeque},
    io,
    num::NonZeroU64,
    str::FromStr as _,
    time::Duration,
};

use ahash::{HashMap, HashMapExt as _};
use bytes::{BufMut as _, Bytes};
use nervix_client_wire::{
    AttachDisposition, AttachOutcome, AttachTransactionRequest, CancelRequest, ClientMessage,
    ClientRequest, CommandDisposition, CommandOutcome, CommandRequest, Diagnostic, NoticeLevel,
    OutcomeOrigin, Reply, ReplyBody, RequestId, RowSchema, ServerEvent, ServerFrame, ServerMessage,
    SessionEndReason, SessionLimits, SubscribeDisposition, SubscribeRequest, SubscriptionHandle,
    SubscriptionType, TransferAssembly, UnsubscribeDisposition, UnsubscribeRequest, UploadChunk,
    UploadReply, UploadStart, VerifiedFrame,
    grpc::{
        ClientExchangeCodec, ClientUploadCodec, EXCHANGE_PATH, FrameDecoder, UPLOAD_RESOURCE_PATH,
    },
};
use nervix_models::{
    CommandExecutionReference, DomainName, ResourceName, ResourceUploadIdentity, SubscriptionName,
    TransactionPosition, TransactionStatus,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement_sources};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Status, Streaming,
    codec::{Codec, EncodeBuf, Encoder},
    codegen::http,
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

use super::cluster::{dev_tls_ca_pem, test_basic_authorization};

/// How long a session waits for the reply to a request it just sent.
const REPLY_TIMEOUT: Duration = Duration::from_secs(120);

/// A row a subscription delivered, as a client displays it.
#[derive(Debug, Clone)]
pub(crate) struct TestSubscriptionEvent {
    pub payload: String,
}

/// A command's outcome together with the size of the frames that carried it.
#[derive(Debug)]
pub(crate) struct TestCommandObservation {
    pub(crate) result: CommandOutcome,
    pub(crate) request_frame_bytes: usize,
    pub(crate) response_frame_bytes: usize,
}

/// A notice, or a subscription failure, the server sent without a request.
#[derive(Debug, Clone)]
pub(crate) struct TestServerEvent {
    pub(crate) level: NoticeLevel,
    pub(crate) message: String,
}

/// A subscription the session opened, and the schema its rows are read against.
#[derive(Debug)]
struct OpenSubscription {
    schema: RowSchema,
}

/// A reply and the bytes of the frames that carried it.
struct ReceivedReply {
    body: ReplyBody,
    frame_bytes: usize,
}

/// Sends frames as they are given, so a scenario can send bytes that are not a valid frame.
#[derive(Debug, Clone, Copy)]
struct RawFrameCodec {
    limits: SessionLimits,
}

struct RawFrameEncoder;

impl Encoder for RawFrameEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&item);
        Ok(())
    }
}

impl Codec for RawFrameCodec {
    type Encode = Bytes;
    type Decode = VerifiedFrame<ServerFrame>;
    type Encoder = RawFrameEncoder;
    type Decoder = FrameDecoder<ServerFrame>;

    fn encoder(&mut self) -> Self::Encoder {
        RawFrameEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        ClientExchangeCodec::new(self.limits).decoder()
    }
}

/// One native session a scenario drives.
pub(crate) struct TestSession {
    domain: Option<DomainName>,
    limits: SessionLimits,
    transaction: Option<TransactionStatus>,
    frames: mpsc::Sender<Bytes>,
    responses: Streaming<VerifiedFrame<ServerFrame>>,
    next_request: NonZeroU64,
    replies: BTreeMap<RequestId, ReceivedReply>,
    transfers: BTreeMap<RequestId, TransferAssembly>,
    subscriptions: HashMap<SubscriptionHandle, OpenSubscription>,
    pending_subscriptions: VecDeque<TestSubscriptionEvent>,
    pending_server_errors: VecDeque<TestServerEvent>,
    /// Why the server said it ends the session, once it said so.
    ending: Option<SessionEndReason>,
    /// The status the server ended the call with, once it ended it.
    ended: Option<Status>,
}

impl std::fmt::Debug for TestSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestSession")
            .field("domain", &self.domain)
            .field("transaction", &self.transaction)
            .finish_non_exhaustive()
    }
}

/// A channel to `server`, trusting the development CA for `https` servers.
pub(crate) async fn session_channel(server: &str) -> io::Result<Channel> {
    let mut endpoint = Endpoint::from_shared(server.to_string()).map_err(io::Error::other)?;
    if server.starts_with("https://") {
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(dev_tls_ca_pem()?)),
            )
            .map_err(io::Error::other)?;
    }
    endpoint.connect().await.map_err(io::Error::other)
}

fn authorized<T>(message: T) -> io::Result<Request<T>> {
    authorized_as(message, &test_basic_authorization())
}

fn authorized_as<T>(message: T, authorization: &str) -> io::Result<Request<T>> {
    let mut request = Request::new(message);
    let authorization = MetadataValue::from_str(authorization).map_err(io::Error::other)?;
    request
        .metadata_mut()
        .insert("authorization", authorization);
    Ok(request)
}

fn parse_domain(domain: &str) -> Option<DomainName> {
    if domain.is_empty() {
        return None;
    }
    Some(DomainName::parse(domain).expect("scenario domains are valid domain names"))
}

/// A fresh execution reference, as a client generates one for each command.
pub(crate) fn fresh_execution_reference() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn execution_reference(raw: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(raw).expect("scenario execution references are valid")
}

/// Opens a session on `server`, sending `domain` with every command.
pub(crate) async fn open_raw_session(server: &str, domain: &str) -> io::Result<TestSession> {
    let opened = open_session_as(server, domain, &test_basic_authorization()).await?;
    opened.map_err(|status| io::Error::other(*status))
}

/// Opens a session on `server` presenting `authorization`. The outer error is the harness's own
/// failure; the inner one is the status the server refused the call with.
pub(crate) async fn open_session_as(
    server: &str,
    domain: &str,
    authorization: &str,
) -> io::Result<Result<TestSession, Box<Status>>> {
    let limits = SessionLimits::DEFAULT;
    let channel = session_channel(server).await?;
    // The harness sends whatever a scenario asks it to, including messages above the frame limit
    // the server must refuse, so only what it receives is held to that limit.
    let mut client =
        tonic::client::Grpc::new(channel).max_decoding_message_size(limits.frame_bytes());
    client.ready().await.map_err(io::Error::other)?;
    let (frames, outgoing) = mpsc::channel(64);
    let request = authorized_as(ReceiverStream::new(outgoing), authorization)?;
    let opened = client
        .streaming(
            request,
            http::uri::PathAndQuery::from_static(EXCHANGE_PATH),
            RawFrameCodec { limits },
        )
        .await;
    let response = match opened {
        Ok(response) => response,
        Err(status) => return Ok(Err(Box::new(status))),
    };
    Ok(Ok(TestSession {
        domain: parse_domain(domain),
        limits,
        transaction: None,
        frames,
        responses: response.into_inner(),
        next_request: NonZeroU64::MIN,
        replies: BTreeMap::new(),
        transfers: BTreeMap::new(),
        subscriptions: HashMap::new(),
        pending_subscriptions: VecDeque::new(),
        pending_server_errors: VecDeque::new(),
        ending: None,
        ended: None,
    }))
}

/// One frame of an upload stream a scenario shapes itself.
pub(crate) enum TestUploadPart {
    /// An upload start for the upload's resource and identity, declaring the archive's size.
    Start {
        declared_bytes: NonZeroU64,
    },
    Chunk(Vec<u8>),
}

/// What an upload stream carries, for uploads a scenario shapes itself. The parts are sent in
/// order, so a scenario can send a stream the protocol does not allow.
pub(crate) struct TestUpload<'a> {
    pub(crate) domain: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) identity: &'a str,
    pub(crate) parts: Vec<TestUploadPart>,
}

/// Streams one upload to `server` and returns the server's reply.
pub(crate) async fn send_upload(server: &str, upload: TestUpload<'_>) -> io::Result<UploadReply> {
    let limits = SessionLimits::DEFAULT;
    let channel = session_channel(server).await?;
    let mut client = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(limits.frame_bytes())
        .max_encoding_message_size(limits.frame_bytes());
    client.ready().await.map_err(io::Error::other)?;
    let domain = DomainName::parse(upload.domain).map_err(io::Error::other)?;
    let resource = ResourceName::parse(upload.resource).map_err(io::Error::other)?;
    let upload_identity =
        ResourceUploadIdentity::parse(upload.identity).map_err(io::Error::other)?;
    let mut frames = Vec::with_capacity(upload.parts.len());
    for part in &upload.parts {
        let frame = match part {
            TestUploadPart::Start { declared_bytes } => {
                let start = UploadStart {
                    request_id: RequestId::new(NonZeroU64::MIN),
                    domain: domain.clone(),
                    resource: resource.clone(),
                    upload_identity: upload_identity.clone(),
                    total_bytes: *declared_bytes,
                };
                start.encode(&limits)
            }
            TestUploadPart::Chunk(bytes) => UploadChunk::encode(bytes, &limits),
        };
        frames.push(frame.map_err(io::Error::other)?);
    }
    let request = authorized(tokio_stream::iter(frames))?;
    let response = client
        .client_streaming(
            request,
            http::uri::PathAndQuery::from_static(UPLOAD_RESOURCE_PATH),
            ClientUploadCodec::new(limits),
        )
        .await
        .map_err(io::Error::other)?;
    UploadReply::decode(response.get_ref()).map_err(io::Error::other)
}

/// A command outcome for a subscription statement the session sent as its own request.
fn subscription_outcome(
    execution_reference: CommandExecutionReference,
    succeeded: bool,
    message: String,
    diagnostics: Vec<Diagnostic>,
) -> CommandOutcome {
    let disposition = if succeeded {
        CommandDisposition::Completed {
            already_existed: false,
        }
    } else {
        CommandDisposition::Failed
    };
    CommandOutcome {
        execution_reference,
        origin: OutcomeOrigin::Executed,
        disposition,
        message,
        diagnostics,
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
    }
}

/// Whether a command completed.
pub(crate) fn outcome_succeeded(outcome: &CommandOutcome) -> bool {
    matches!(outcome.disposition, CommandDisposition::Completed { .. })
}

/// The questions scenarios ask of a wire outcome, spelled the way the native client spells them.
pub(crate) trait WireOutcome {
    /// Whether the request completed.
    fn succeeded(&self) -> bool;
}

impl WireOutcome for CommandOutcome {
    fn succeeded(&self) -> bool {
        outcome_succeeded(self)
    }
}

impl WireOutcome for AttachOutcome {
    fn succeeded(&self) -> bool {
        matches!(self.disposition, AttachDisposition::Attached(_))
    }
}

/// Renders a batched command outcome as its aggregate message followed by every non-empty
/// statement message.
pub(crate) fn flatten_command_messages(outcome: &CommandOutcome) -> String {
    let mut messages = Vec::new();
    if !outcome.message.is_empty() {
        messages.push(outcome.message.clone());
    }
    for statement in &outcome.statements {
        if !statement.message.is_empty() {
            messages.push(statement.message.clone());
        }
    }
    messages.join("\n")
}

/// How a command's text is sent.
enum CommandRoute {
    Command,
    Subscribe,
    Unsubscribe(SubscriptionName),
}

impl TestSession {
    pub(crate) fn set_domain(&mut self, domain: String) {
        self.domain = parse_domain(&domain);
    }

    fn next_request_id(&mut self) -> RequestId {
        let id = RequestId::new(self.next_request);
        self.next_request = self
            .next_request
            .checked_add(1)
            .expect("a test session sends fewer than u64::MAX requests");
        id
    }

    /// Sends one request and returns its identity and the bytes of its frame.
    pub(crate) async fn send_request(
        &mut self,
        request: ClientRequest,
    ) -> io::Result<(RequestId, usize)> {
        let request_id = self.next_request_id();
        let message = ClientMessage {
            request_id,
            request,
        };
        let frame = message.encode(&self.limits).map_err(io::Error::other)?;
        let bytes = frame.into_bytes();
        let frame_bytes = bytes.len();
        self.send_raw_frame(bytes).await?;
        Ok((request_id, frame_bytes))
    }

    /// Sends bytes as one gRPC message, whether or not they are a valid frame.
    pub(crate) async fn send_raw_frame(&mut self, bytes: Bytes) -> io::Result<()> {
        self.frames
            .send(bytes)
            .await
            .map_err(|_| io::Error::other("the session's request stream closed"))
    }

    /// Reads one frame and files it. `false` means the call ended.
    async fn read_frame(&mut self) -> io::Result<bool> {
        if self.ended.is_some() {
            return Ok(false);
        }
        let frame = match self.responses.message().await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                self.ended = Some(Status::ok("the server ended the session"));
                return Ok(false);
            }
            Err(status) => {
                self.ended = Some(status);
                return Ok(false);
            }
        };
        let frame_bytes = frame.bytes().len();
        let message = ServerMessage::decode(&frame).map_err(io::Error::other)?;
        match message {
            ServerMessage::Reply(reply) => self.file_reply(reply, frame_bytes),
            ServerMessage::TransferPart(part) => {
                let request_id = part.request_id();
                let limits = self.limits;
                let assembly = self
                    .transfers
                    .entry(request_id)
                    .or_insert_with(|| TransferAssembly::new(request_id, &limits));
                assembly.append(&part).map_err(io::Error::other)?;
                if assembly.is_complete() {
                    let assembly = self
                        .transfers
                        .remove(&request_id)
                        .expect("the assembly was just appended to");
                    let total = assembly.received_bytes();
                    let reply = assembly.finish().map_err(io::Error::other)?;
                    self.file_reply(reply, total);
                }
            }
            ServerMessage::Event(event) => self.file_event(event)?,
        }
        Ok(true)
    }

    fn file_reply(&mut self, reply: Reply, frame_bytes: usize) {
        let Reply { request_id, body } = reply;
        match &body {
            ReplyBody::Subscribe(outcome) => {
                if let SubscribeDisposition::Opened(opened) = &outcome.disposition {
                    self.subscriptions.insert(
                        opened.subscription.clone(),
                        OpenSubscription {
                            schema: opened.schema.clone(),
                        },
                    );
                }
            }
            ReplyBody::Unsubscribe(outcome) => {
                if let UnsubscribeDisposition::Deleted(handle) = &outcome.disposition {
                    self.subscriptions.remove(handle);
                }
            }
            _ => {}
        }
        self.replies
            .insert(request_id, ReceivedReply { body, frame_bytes });
    }

    fn file_event(&mut self, event: ServerEvent) -> io::Result<()> {
        match event {
            ServerEvent::Notice(notice) => {
                if let NoticeLevel::Error = notice.level {
                    self.pending_server_errors.push_back(TestServerEvent {
                        level: notice.level,
                        message: notice.message,
                    });
                }
            }
            ServerEvent::SubscriptionRows(rows) => {
                let Some(subscription) = self.subscriptions.get(rows.subscription()) else {
                    return Ok(());
                };
                let lines = rows
                    .batch()
                    .display_lines(&subscription.schema)
                    .map_err(io::Error::other)?;
                for payload in lines {
                    self.pending_subscriptions
                        .push_back(TestSubscriptionEvent { payload });
                }
            }
            ServerEvent::SubscriptionRowsSkipped(skipped) => {
                self.pending_server_errors.push_back(TestServerEvent {
                    level: NoticeLevel::Error,
                    message: skipped.message,
                });
            }
            ServerEvent::SubscriptionEnded(ended) => {
                self.subscriptions.remove(&ended.subscription);
                self.pending_server_errors.push_back(TestServerEvent {
                    level: NoticeLevel::Error,
                    message: ended.message,
                });
            }
            ServerEvent::SessionEnding(ending) => {
                self.ending = Some(ending.reason);
            }
            ServerEvent::SubscriptionDeliveryLost(_)
            | ServerEvent::Leadership(_)
            | ServerEvent::Domains(_)
            | ServerEvent::DomainSnapshot(_)
            | ServerEvent::Cluster(_) => {}
        }
        Ok(())
    }

    /// Waits for the terminal reply of `request_id`.
    pub(crate) async fn reply_to(&mut self, request_id: RequestId) -> io::Result<ReplyBody> {
        let reply = self.received_reply(request_id).await?;
        Ok(reply.body)
    }

    async fn received_reply(&mut self, request_id: RequestId) -> io::Result<ReceivedReply> {
        let deadline = Instant::now() + REPLY_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            if let Some(reply) = self.replies.remove(&request_id) {
                return Ok(reply);
            }
            let read = tokio::time::timeout_at(deadline, self.read_frame()).await;
            let open = match read {
                Ok(open) => open?,
                Err(_) => {
                    return Err(io::Error::other(format!(
                        "timed out waiting for the reply to request {request_id}"
                    )));
                }
            };
            if !open {
                return Err(io::Error::other(format!(
                    "the session ended before request {request_id} was answered: {:?}",
                    self.ended
                )));
            }
        }
    }

    fn route(query: &str) -> io::Result<CommandRoute> {
        let Ok(statements) = parse_client_statement_sources(query) else {
            return Ok(CommandRoute::Command);
        };
        let subscription_statements = statements
            .iter()
            .filter(|parsed| {
                matches!(
                    parsed.statement,
                    ClientStatement::CreateSubscription(_) | ClientStatement::DeleteSubscription(_)
                )
            })
            .count();
        if subscription_statements == 0 {
            return Ok(CommandRoute::Command);
        }
        if statements.len() != 1 {
            return Err(io::Error::other(
                "the harness sends a subscription statement as its own request",
            ));
        }
        let Some(parsed) = statements.into_iter().next() else {
            return Ok(CommandRoute::Command);
        };
        match parsed.statement {
            ClientStatement::CreateSubscription(_) => Ok(CommandRoute::Subscribe),
            ClientStatement::DeleteSubscription(delete) => {
                Ok(CommandRoute::Unsubscribe(delete.name))
            }
            _ => Ok(CommandRoute::Command),
        }
    }

    fn command_request(
        &self,
        query: &str,
        execution_reference: &str,
        expected_transaction_position: Option<TransactionPosition>,
    ) -> CommandRequest {
        CommandRequest {
            query: query.to_string(),
            domain: self.domain.clone(),
            execution_reference: self::execution_reference(execution_reference),
            expected_transaction_position,
            expected_preview: None,
        }
    }

    /// The position the next append to the bound transaction expects.
    fn expected_position(&self) -> Option<TransactionPosition> {
        let transaction = self.transaction.as_ref()?;
        if !transaction.lifecycle().is_active() {
            return None;
        }
        Some(transaction.accepted_operations())
    }

    /// Sends a command without waiting for its reply, and returns the request's identity.
    pub(crate) async fn send_command_request_with_reference(
        &mut self,
        query: &str,
        execution_reference: &str,
    ) -> io::Result<RequestId> {
        let request = self.command_request(query, execution_reference, self.expected_position());
        let (request_id, _) = self.send_request(ClientRequest::Command(request)).await?;
        Ok(request_id)
    }

    /// The domain the session sends with its commands.
    pub(crate) fn domain(&self) -> Option<&DomainName> {
        self.domain.as_ref()
    }

    /// Sends `original` with the bytes that tell it apart from `variant` replaced by
    /// `replacement`.
    ///
    /// Both messages must encode to frames of one size that differ only in the one field the
    /// scenario alters, so every byte outside that field stays a valid part of the frame. This is
    /// how a scenario sends a value no typed request can hold, such as a cursor inside a
    /// character.
    pub(crate) async fn send_altered(
        &mut self,
        original: &ClientMessage,
        variant: &ClientMessage,
        replacement: &[u8],
    ) -> io::Result<()> {
        let original = original.encode(&self.limits).map_err(io::Error::other)?;
        let variant = variant.encode(&self.limits).map_err(io::Error::other)?;
        let original = original.into_bytes();
        let variant = variant.into_bytes();
        if original.len() != variant.len() {
            return Err(io::Error::other(
                "the altered field changes the size of the frame",
            ));
        }
        let mut differing = Vec::new();
        for (index, (left, right)) in original.iter().zip(variant.iter()).enumerate() {
            if left != right {
                differing.push(index);
            }
        }
        let (Some(&first), Some(&last)) = (differing.first(), differing.last()) else {
            return Err(io::Error::other("the variant does not alter the frame"));
        };
        if last - first + 1 != replacement.len() {
            return Err(io::Error::other(format!(
                "the altered field spans {} bytes, not the {} bytes of its replacement",
                last - first + 1,
                replacement.len()
            )));
        }
        let mut altered = original.to_vec();
        altered[first..=last].copy_from_slice(replacement);
        self.send_raw_frame(Bytes::from(altered)).await
    }

    /// The next request identity this session would use, reserved for a request a scenario
    /// encodes itself.
    pub(crate) fn reserve_request_id(&mut self) -> RequestId {
        self.next_request_id()
    }

    /// Why the server said it ends the session, once it said so.
    pub(crate) fn ending(&self) -> Option<&SessionEndReason> {
        self.ending.as_ref()
    }

    pub(crate) async fn run_command(&mut self, query: &str) -> io::Result<String> {
        let result = self.run_command_result(query).await?;
        if outcome_succeeded(&result) {
            return Ok(flatten_command_messages(&result));
        }
        Err(io::Error::other(format!(
            "command failed: {}\ndiagnostics: {:?}",
            result.message, result.diagnostics
        )))
    }

    pub(crate) async fn run_command_result(&mut self, query: &str) -> io::Result<CommandOutcome> {
        let execution_reference = fresh_execution_reference();
        self.run_command_result_with_reference(query, &execution_reference)
            .await
    }

    pub(crate) async fn run_command_result_with_reference(
        &mut self,
        query: &str,
        execution_reference: &str,
    ) -> io::Result<CommandOutcome> {
        let observation = self
            .observe_command_with_reference(query, execution_reference, self.expected_position())
            .await?;
        Ok(observation.result)
    }

    pub(crate) async fn run_command_result_with_reference_at_position(
        &mut self,
        query: &str,
        execution_reference: &str,
        expected_transaction_position: usize,
    ) -> io::Result<CommandOutcome> {
        let observation = self
            .observe_command_with_reference(
                query,
                execution_reference,
                Some(TransactionPosition::new(expected_transaction_position)),
            )
            .await?;
        Ok(observation.result)
    }

    pub(crate) async fn observe_command(
        &mut self,
        query: &str,
    ) -> io::Result<TestCommandObservation> {
        let execution_reference = fresh_execution_reference();
        self.observe_command_with_reference(query, &execution_reference, self.expected_position())
            .await
    }

    async fn observe_command_with_reference(
        &mut self,
        query: &str,
        execution_reference: &str,
        expected_transaction_position: Option<TransactionPosition>,
    ) -> io::Result<TestCommandObservation> {
        match Self::route(query)? {
            CommandRoute::Command => {}
            CommandRoute::Subscribe => {
                return self.subscribe(query, execution_reference).await;
            }
            CommandRoute::Unsubscribe(name) => {
                return self.unsubscribe(name, execution_reference).await;
            }
        }
        let request =
            self.command_request(query, execution_reference, expected_transaction_position);
        let (request_id, request_frame_bytes) =
            self.send_request(ClientRequest::Command(request)).await?;
        let reply = self.received_reply(request_id).await?;
        let ReplyBody::Command(outcome) = reply.body else {
            return Err(io::Error::other(format!(
                "a command was answered with {:?}",
                reply.body
            )));
        };
        if outcome.transaction.is_some() {
            self.transaction = outcome.transaction.clone();
        }
        if let Some(transaction) = &outcome.transaction
            && !transaction.lifecycle().is_active()
        {
            self.transaction = None;
        }
        Ok(TestCommandObservation {
            result: *outcome,
            request_frame_bytes,
            response_frame_bytes: reply.frame_bytes,
        })
    }

    async fn subscribe(
        &mut self,
        statement: &str,
        execution_reference: &str,
    ) -> io::Result<TestCommandObservation> {
        let domain = self
            .domain
            .clone()
            .ok_or_else(|| io::Error::other("a subscription needs an active domain"))?;
        let request = SubscribeRequest {
            domain,
            statement: statement.to_string(),
            subscription_type: SubscriptionType::Row,
        };
        let (request_id, request_frame_bytes) =
            self.send_request(ClientRequest::Subscribe(request)).await?;
        let reply = self.received_reply(request_id).await?;
        let ReplyBody::Subscribe(outcome) = reply.body else {
            return Err(io::Error::other(format!(
                "a subscribe request was answered with {:?}",
                reply.body
            )));
        };
        let succeeded = matches!(outcome.disposition, SubscribeDisposition::Opened(_));
        Ok(TestCommandObservation {
            result: subscription_outcome(
                self::execution_reference(execution_reference),
                succeeded,
                outcome.message,
                outcome.diagnostics,
            ),
            request_frame_bytes,
            response_frame_bytes: reply.frame_bytes,
        })
    }

    async fn unsubscribe(
        &mut self,
        subscription: SubscriptionName,
        execution_reference: &str,
    ) -> io::Result<TestCommandObservation> {
        let request = UnsubscribeRequest { subscription };
        let (request_id, request_frame_bytes) = self
            .send_request(ClientRequest::Unsubscribe(request))
            .await?;
        let reply = self.received_reply(request_id).await?;
        let ReplyBody::Unsubscribe(outcome) = reply.body else {
            return Err(io::Error::other(format!(
                "an unsubscribe request was answered with {:?}",
                reply.body
            )));
        };
        let succeeded = matches!(outcome.disposition, UnsubscribeDisposition::Deleted(_));
        Ok(TestCommandObservation {
            result: subscription_outcome(
                self::execution_reference(execution_reference),
                succeeded,
                outcome.message,
                outcome.diagnostics,
            ),
            request_frame_bytes,
            response_frame_bytes: reply.frame_bytes,
        })
    }

    pub(crate) async fn attach_transaction(
        &mut self,
        transaction_id: &str,
    ) -> io::Result<AttachOutcome> {
        let request = AttachTransactionRequest {
            transaction_id: transaction_id.to_string(),
        };
        let (request_id, _) = self
            .send_request(ClientRequest::AttachTransaction(request))
            .await?;
        let body = self.reply_to(request_id).await?;
        let ReplyBody::Attach(outcome) = body else {
            return Err(io::Error::other(format!(
                "an attach request was answered with {body:?}"
            )));
        };
        if let AttachDisposition::Attached(status) = &outcome.disposition {
            self.transaction = Some(status.clone());
        }
        Ok(outcome)
    }

    /// Asks the server to stop waiting for `target` and returns the cancel request's identity.
    pub(crate) async fn cancel(&mut self, target: RequestId) -> io::Result<RequestId> {
        let (request_id, _) = self
            .send_request(ClientRequest::Cancel(CancelRequest { target }))
            .await?;
        Ok(request_id)
    }

    pub(crate) async fn try_next_subscription(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestSubscriptionEvent>> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            tokio::task::consume_budget().await;
            if let Some(event) = self.pending_subscriptions.pop_front() {
                return Ok(Some(event));
            }
            let read = tokio::time::timeout_at(deadline, self.read_frame()).await;
            let open = match read {
                Ok(open) => open?,
                Err(_) => return Ok(None),
            };
            if !open {
                return Err(io::Error::other(format!(
                    "the session ended before a subscription event: {:?}",
                    self.ended
                )));
            }
        }
    }

    pub(crate) async fn try_next_server_error(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestServerEvent>> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            tokio::task::consume_budget().await;
            if let Some(event) = self.pending_server_errors.pop_front() {
                return Ok(Some(event));
            }
            let read = tokio::time::timeout_at(deadline, self.read_frame()).await;
            let open = match read {
                Ok(open) => open?,
                Err(_) => return Ok(None),
            };
            if !open {
                return Err(io::Error::other(format!(
                    "the session ended before a server event: {:?}",
                    self.ended
                )));
            }
        }
    }

    /// Reads frames until the server ends the call, and returns the status it ended it with.
    pub(crate) async fn wait_until_ended(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Status> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            tokio::task::consume_budget().await;
            if let Some(status) = &self.ended {
                return Ok(status.clone());
            }
            let read = tokio::time::timeout_at(deadline, self.read_frame()).await;
            match read {
                Ok(open) => {
                    open?;
                }
                Err(_) => {
                    return Err(io::Error::other(
                        "timed out waiting for the server to end the session",
                    ));
                }
            }
        }
    }
}
