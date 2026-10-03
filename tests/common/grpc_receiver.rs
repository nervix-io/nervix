//! A unary gRPC receiver that stands in for a service an operator provisioned, such as an OTLP
//! collector, so a scenario can see exactly which calls Nervix made and decide exactly how each is
//! answered.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener serving HTTP/2 without TLS, the connections it accepts, the
//!   scripted answers their calls take, the calls they captured, the faults they observed, and the
//!   bounded stop that ends all of them.
//! - **Depends on.** Tokio, the `h2` server, and gRPC's length-prefixed message framing.
//! - **Must not know.** Scenario state, nodes, NSPL, or what the messages it captures mean.
//!
//! # Scripted answers
//!
//! Every call is read to the end of its request before it is answered, and each one takes the next
//! answer from the script. Once the script is empty, calls are accepted: they receive an empty
//! response message and `grpc-status: 0`, which is a valid answer to any method whose response
//! message has no required field. A call can instead receive a trailers-only status, lose its
//! answer when the receiver closes the whole connection, or be held unanswered until the client
//! resets it. The last two are how a scenario loses an answer to a call the service already
//! received, and holds a call past its deadline.
//!
//! # Bounds
//!
//! A request message, the number of captured calls and the number of recorded faults each have
//! the limit the HTTP receiver has, and exceeding one is recorded as a fault rather than captured.
//! A message whose length prefix already exceeds the limit is refused before its bytes are read.
//! Every wait a connection or call makes observes the receiver's cancellation, and stopping gives
//! connections the HTTP receiver's connection budget to end before it aborts and joins the ones that
//! remain, within the same whole-stop budget.

use std::{collections::VecDeque, fmt, io, net::SocketAddr, time::Duration};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{Arc, CancellationToken, StdArc, blocking::Mutex, watch},
    task::{AbortOnDropHandle, JoinSet},
    time::Instant,
};
use nervix_recovery::Discarded as _;
use thiserror::Error;

use super::http_receiver::{
    AcceptLoopEnding, ConnectionSummary, MAX_CAPTURED_REQUESTS, MAX_RECORDED_FAULTS,
    MAX_REQUEST_BODY_BYTES, RECEIVER_CONNECTION_STOP_BUDGET, RECEIVER_STOP_BUDGET,
};

/// The bytes of a gRPC message prefix: one compressed flag, then a four-byte big-endian length.
const MESSAGE_PREFIX_BYTES: usize = 5;
/// The largest request message the receiver reads, the HTTP receiver's body limit.
pub(crate) const MAX_REQUEST_MESSAGE_BYTES: usize = MAX_REQUEST_BODY_BYTES;

/// One scripted answer to one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrpcAnswer {
    /// An empty response message, then `grpc-status: 0`.
    Accept,
    /// A trailers-only response carrying this status, which is never `OK`.
    Status(tonic::Code),
    /// Capture the call, then close its connection without answering it or any other call on it.
    LoseResponse,
    /// Capture the call, then answer nothing until the client resets it or the receiver stops.
    HoldResponse,
}

/// One call exactly as the receiver read it. Two captures are equal when their paths, their header
/// fields in the order the header map yields them, their compressed flags, and their message bytes
/// are the same.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CapturedCall {
    /// The method the call named, such as `/opentelemetry.proto.collector.logs.v1.LogsService/Export`.
    pub(crate) path: String,
    headers: Vec<CapturedCallHeader>,
    /// Whether the message carried gRPC's compressed flag.
    pub(crate) compressed: bool,
    /// The request message exactly as it arrived, still compressed when `compressed` is set.
    pub(crate) message: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedCallHeader {
    name: String,
    value: Vec<u8>,
}

impl CapturedCall {
    /// Every value sent under `name`, in the order the header map yields them.
    pub(crate) fn header_values(&self, name: &str) -> Vec<&[u8]> {
        let mut values = Vec::new();
        for header in &self.headers {
            if header.name.eq_ignore_ascii_case(name) {
                values.push(header.value.as_slice());
            }
        }
        values
    }
}

impl fmt::Display for CapturedCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "POST {}", self.path)?;
        for header in &self.headers {
            writeln!(
                formatter,
                "{}: {}",
                header.name,
                String::from_utf8_lossy(&header.value)
            )?;
        }
        write!(
            formatter,
            "message: {} byte(s), compressed: {}",
            self.message.len(),
            self.compressed
        )
    }
}

/// Why a call's request was not one gRPC message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum MessageFraming {
    #[error("the request ended inside its message prefix")]
    TruncatedPrefix,
    #[error("the request ended before the {declared} bytes its prefix declared")]
    TruncatedMessage { declared: usize },
    #[error("the request carried bytes after its one message")]
    TrailingBytes,
    #[error("the message prefix carried the unknown compressed flag {flag}")]
    UnknownFlag { flag: u8 },
}

/// Something the receiver observed and could not capture as a call.
#[derive(Clone, Debug, Error)]
pub(crate) enum GrpcReceiverFault {
    #[error("accepting a connection failed")]
    Accept {
        #[source]
        source: StdArc<io::Error>,
    },
    #[error("connection {connection} failed its HTTP/2 handshake")]
    Handshake {
        connection: u64,
        #[source]
        source: StdArc<h2::Error>,
    },
    #[error("connection {connection} failed")]
    Connection {
        connection: u64,
        #[source]
        source: StdArc<h2::Error>,
    },
    #[error("a call on connection {connection} failed while its request was read")]
    Request {
        connection: u64,
        #[source]
        source: StdArc<h2::Error>,
    },
    #[error("a call on connection {connection} declared a message above the {limit}-byte limit")]
    MessageTooLarge { connection: u64, limit: usize },
    #[error("a call on connection {connection} was not one gRPC message: {framing}")]
    MalformedMessage {
        connection: u64,
        framing: MessageFraming,
    },
    #[error(
        "a call on connection {connection} arrived after the receiver captured its limit of \
         {limit}"
    )]
    CaptureLimit { connection: u64, limit: usize },
}

/// A receiver could not start.
#[derive(Debug, Error)]
pub(crate) enum GrpcReceiverStartError {
    #[error("binding the gRPC receiver to {address} failed")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
}

/// A wait on a receiver ended at its deadline.
#[derive(Debug, Error)]
#[error(
    "the gRPC receiver captured {captured} of the {expected} calls expected within {waited:?}; \
     {faults} fault(s), the latest: {latest_fault}"
)]
pub(crate) struct GrpcReceiverWaitError {
    expected: usize,
    captured: usize,
    waited: Duration,
    faults: usize,
    latest_fault: LatestGrpcFault,
}

/// The most recent fault a failed wait reports, or that there was none.
#[derive(Debug)]
pub(crate) enum LatestGrpcFault {
    None,
    Fault(GrpcReceiverFault),
}

impl fmt::Display for LatestGrpcFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(formatter, "none"),
            Self::Fault(fault) => write!(formatter, "{fault}"),
        }
    }
}

/// What every connection of one receiver shares.
struct GrpcReceiverState {
    script: Mutex<VecDeque<GrpcAnswer>>,
    captured: Mutex<Vec<CapturedCall>>,
    faults: Mutex<RecordedGrpcFaults>,
    captured_count: watch::Sender<usize>,
    fault_count: watch::Sender<usize>,
}

#[derive(Default)]
struct RecordedGrpcFaults {
    kept: Vec<GrpcReceiverFault>,
    total: usize,
}

/// Whether the receiver kept a call or had already captured its limit.
enum Capture {
    Kept,
    AtLimit,
}

impl GrpcReceiverState {
    fn new() -> Self {
        Self {
            script: Mutex::new(VecDeque::new()),
            captured: Mutex::new(Vec::new()),
            faults: Mutex::new(RecordedGrpcFaults::default()),
            captured_count: watch::Sender::new(0),
            fault_count: watch::Sender::new(0),
        }
    }

    fn next_answer(&self) -> GrpcAnswer {
        match self.script.lock().pop_front() {
            Some(answer) => answer,
            None => GrpcAnswer::Accept,
        }
    }

    fn capture(&self, call: CapturedCall) -> Capture {
        // The capture lock is released before the count is published: a call wait reads the
        // captures while it can still hold the count's read lock.
        let count = {
            let mut captured = self.captured.lock();
            if captured.len() >= MAX_CAPTURED_REQUESTS {
                return Capture::AtLimit;
            }
            captured.push(call);
            captured.len()
        };
        self.captured_count.send_replace(count);
        Capture::Kept
    }

    fn record(&self, fault: GrpcReceiverFault) {
        // The fault lock is released before the count is published, for the same reason.
        let total = {
            let mut faults = self.faults.lock();
            faults.total = faults
                .total
                .checked_add(1)
                .assured("one receiver cannot observe more faults than the address space holds");
            if faults.kept.len() < MAX_RECORDED_FAULTS {
                faults.kept.push(fault);
            }
            faults.total
        };
        self.fault_count.send_replace(total);
    }

    fn latest_fault(&self) -> LatestGrpcFault {
        match self.faults.lock().kept.last() {
            Some(fault) => LatestGrpcFault::Fault(fault.clone()),
            None => LatestGrpcFault::None,
        }
    }

    fn fault_total(&self) -> usize {
        self.faults.lock().total
    }
}

/// A running receiver. Dropping it aborts every task it started; [`GrpcReceiver::stop`] ends them
/// within [`RECEIVER_STOP_BUDGET`] and reports how.
pub(crate) struct GrpcReceiver {
    address: SocketAddr,
    state: Arc<GrpcReceiverState>,
    cancellation: CancellationToken,
    accept_loop: AbortOnDropHandle<ConnectionSummary>,
}

impl fmt::Debug for GrpcReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcReceiver")
            .field("address", &self.address)
            .field("captured", &*self.state.captured_count.borrow())
            .field("faults", &*self.state.fault_count.borrow())
            .finish()
    }
}

impl GrpcReceiver {
    pub(crate) async fn start(address: SocketAddr) -> Result<Self, GrpcReceiverStartError> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| GrpcReceiverStartError::Bind { address, source })?;
        let address = listener
            .local_addr()
            .map_err(|source| GrpcReceiverStartError::Bind { address, source })?;
        let state = Arc::new(GrpcReceiverState::new());
        let cancellation = CancellationToken::new();
        let accept_loop = GrpcAcceptLoop {
            listener,
            state: state.clone(),
            cancellation: cancellation.clone(),
        };
        Ok(Self {
            address,
            state,
            cancellation,
            accept_loop: AbortOnDropHandle::new(nervix_primitives::task::spawn(accept_loop.run())),
        })
    }

    /// The origin a client dials, `http://127.0.0.1:<port>`.
    pub(crate) fn origin(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn port(&self) -> u16 {
        self.address.port()
    }

    /// Appends answers the next calls take, in order.
    pub(crate) fn script(&self, answers: impl IntoIterator<Item = GrpcAnswer>) {
        self.state.script.lock().extend(answers);
    }

    pub(crate) fn captured(&self) -> Vec<CapturedCall> {
        self.state.captured.lock().clone()
    }

    /// Waits until the receiver has captured at least `expected` calls.
    pub(crate) async fn wait_for_calls(
        &self,
        expected: usize,
        within: Duration,
    ) -> Result<Vec<CapturedCall>, GrpcReceiverWaitError> {
        let mut captured_count = self.state.captured_count.subscribe();
        let waited = nervix_primitives::time::timeout(
            within,
            captured_count.wait_for(|count| *count >= expected),
        )
        .await;
        match waited {
            Ok(Ok(_)) => Ok(self.captured()),
            Ok(Err(_)) | Err(_) => Err(GrpcReceiverWaitError {
                expected,
                captured: *self.state.captured_count.borrow(),
                waited: within,
                faults: self.state.fault_total(),
                latest_fault: self.state.latest_fault(),
            }),
        }
    }

    /// Stops accepting, ends every connection, and reports how the stop went.
    pub(crate) async fn stop(self) -> GrpcReceiverStop {
        let started = Instant::now();
        self.cancellation.cancel();
        let Self {
            state,
            mut accept_loop,
            ..
        } = self;
        let joined = nervix_primitives::time::timeout(RECEIVER_STOP_BUDGET, &mut accept_loop).await;
        let ending = match joined {
            Ok(Ok(connections)) => AcceptLoopEnding::Returned(connections),
            Ok(Err(error)) => AcceptLoopEnding::Failed(error),
            Err(_) => {
                accept_loop.abort();
                match accept_loop.await {
                    Ok(connections) => AcceptLoopEnding::Returned(connections),
                    Err(error) if error.is_cancelled() => AcceptLoopEnding::Aborted,
                    Err(error) => AcceptLoopEnding::Failed(error),
                }
            }
        };
        GrpcReceiverStop {
            elapsed: started.elapsed(),
            captured: *state.captured_count.borrow(),
            faults: state.fault_total(),
            ending,
        }
    }
}

/// How one receiver's stop went.
#[derive(Debug)]
pub(crate) struct GrpcReceiverStop {
    pub(crate) elapsed: Duration,
    pub(crate) captured: usize,
    pub(crate) faults: usize,
    pub(crate) ending: AcceptLoopEnding,
}

impl GrpcReceiverStop {
    /// Whether anything had to be aborted, or panicked, rather than end on its own.
    pub(crate) fn was_forced(&self) -> bool {
        match &self.ending {
            AcceptLoopEnding::Returned(connections) => {
                connections.forced > 0 || connections.panicked > 0
            }
            AcceptLoopEnding::Aborted | AcceptLoopEnding::Failed(_) => true,
        }
    }
}

impl fmt::Display for GrpcReceiverStop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.ending {
            AcceptLoopEnding::Returned(connections) => write!(
                formatter,
                "stopped {} connection(s) in {:?} of a {:?} budget, {} forced, {} panicked",
                connections.served,
                self.elapsed,
                RECEIVER_STOP_BUDGET,
                connections.forced,
                connections.panicked
            )?,
            AcceptLoopEnding::Aborted => write!(
                formatter,
                "the accept loop was still running at the {RECEIVER_STOP_BUDGET:?} budget and was \
                 aborted and joined"
            )?,
            AcceptLoopEnding::Failed(error) => {
                write!(formatter, "the accept loop failed: {error}")?;
            }
        }
        write!(
            formatter,
            "; captured {} call(s), recorded {} fault(s)",
            self.captured, self.faults
        )
    }
}

struct GrpcAcceptLoop {
    listener: TcpListener,
    state: Arc<GrpcReceiverState>,
    cancellation: CancellationToken,
}

impl GrpcAcceptLoop {
    async fn run(self) -> ConnectionSummary {
        let mut connections = JoinSet::new();
        let mut summary = ConnectionSummary::default();
        let mut next_connection = 0_u64;
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    summary.joined(joined);
                }
                accepted = self.listener.accept() => {
                    let stream = match accepted {
                        Ok((stream, _)) => stream,
                        Err(source) => {
                            self.state.record(GrpcReceiverFault::Accept {
                                source: StdArc::new(source),
                            });
                            continue;
                        }
                    };
                    let connection = GrpcConnection {
                        id: next_connection,
                        state: self.state.clone(),
                        cancellation: self.cancellation.clone(),
                    };
                    next_connection = next_connection
                        .checked_add(1)
                        .assured("a receiver cannot accept 2^64 connections");
                    summary.started();
                    connections.spawn(connection.serve(stream));
                }
            }
        }
        let deadline = nervix_primitives::time::Instant::now() + RECEIVER_CONNECTION_STOP_BUDGET;
        loop {
            nervix_primitives::task::consume_budget().await;
            match nervix_primitives::time::timeout_at(deadline, connections.join_next()).await {
                Ok(Some(joined)) => summary.joined(joined),
                Ok(None) => break,
                Err(_) => {
                    connections.abort_all();
                    while let Some(joined) = connections.join_next().await {
                        nervix_primitives::task::consume_budget().await;
                        summary.joined(joined);
                    }
                    break;
                }
            }
        }
        summary
    }
}

/// One accepted connection and the calls it carries.
struct GrpcConnection {
    id: u64,
    state: Arc<GrpcReceiverState>,
    cancellation: CancellationToken,
}

impl GrpcConnection {
    /// Serves calls until the client closes the connection, a call loses its answer, or the
    /// receiver stops. The connection is dropped at the end, which closes its socket without
    /// flushing anything more to the client.
    async fn serve(self, stream: TcpStream) {
        let handshake = nervix_primitives::select! {
            () = self.cancellation.cancelled() => return,
            handshake = h2::server::handshake(stream) => handshake,
        };
        let mut connection = match handshake {
            Ok(connection) => connection,
            Err(source) => {
                self.state.record(GrpcReceiverFault::Handshake {
                    connection: self.id,
                    source: StdArc::new(source),
                });
                return;
            }
        };
        let lost = CancellationToken::new();
        let mut calls = JoinSet::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                () = lost.cancelled() => break,
                Some(joined) = calls.join_next(), if !calls.is_empty() => {
                    // A call that panicked takes its connection with it, so the accept loop counts
                    // the panic when it joins the connection.
                    if let Err(error) = joined
                        && error.is_panic()
                    {
                        std::panic::resume_unwind(error.into_panic());
                    }
                }
                accepted = connection.accept() => {
                    let (request, respond) = match accepted {
                        None => break,
                        Some(Ok(call)) => call,
                        Some(Err(source)) => {
                            self.state.record(GrpcReceiverFault::Connection {
                                connection: self.id,
                                source: StdArc::new(source),
                            });
                            break;
                        }
                    };
                    let call = GrpcCall {
                        connection: self.id,
                        state: self.state.clone(),
                        cancellation: self.cancellation.clone(),
                        lost: lost.clone(),
                    };
                    calls.spawn(call.serve(request, respond));
                }
            }
        }
        // A call that lost its answer, or one still held or reading, ends with the connection. Its
        // unanswered stream is never flushed, because the connection is not polled again.
        calls.abort_all();
        while calls.join_next().await.is_some() {
            nervix_primitives::task::consume_budget().await;
        }
        drop(connection);
    }
}

/// One call on a connection.
struct GrpcCall {
    connection: u64,
    state: Arc<GrpcReceiverState>,
    cancellation: CancellationToken,
    /// Cancelled when this call's answer is lost, which ends the connection it arrived on.
    lost: CancellationToken,
}

impl GrpcCall {
    async fn serve(
        self,
        request: http::Request<h2::RecvStream>,
        mut respond: h2::server::SendResponse<Bytes>,
    ) {
        let (head, mut body) = request.into_parts();
        let message = match self.read_message(&mut body).await {
            Ok(Some(message)) => message,
            Ok(None) => return,
            Err(fault) => {
                self.state.record(fault);
                self.respond_with_status(&mut respond, tonic::Code::Internal);
                return;
            }
        };
        let mut headers = Vec::with_capacity(head.headers.len());
        for (name, value) in &head.headers {
            headers.push(CapturedCallHeader {
                name: name.as_str().to_string(),
                value: value.as_bytes().to_vec(),
            });
        }
        let call = CapturedCall {
            path: head.uri.path().to_string(),
            headers,
            compressed: message.compressed,
            message: message.bytes,
        };
        match self.state.capture(call) {
            Capture::Kept => {}
            Capture::AtLimit => {
                self.state.record(GrpcReceiverFault::CaptureLimit {
                    connection: self.connection,
                    limit: MAX_CAPTURED_REQUESTS,
                });
                self.respond_with_status(&mut respond, tonic::Code::ResourceExhausted);
                return;
            }
        }
        match self.state.next_answer() {
            GrpcAnswer::Accept => self.accept(&mut respond),
            GrpcAnswer::Status(code) => self.respond_with_status(&mut respond, code),
            GrpcAnswer::LoseResponse => {
                self.lost.cancel();
                // The unanswered stream stays open until the connection that carries it is gone.
                std::future::pending::<()>().await;
            }
            GrpcAnswer::HoldResponse => {
                nervix_primitives::select! {
                    () = self.cancellation.cancelled() => {}
                    _ = std::future::poll_fn(|context| respond.poll_reset(context)) => {}
                }
            }
        }
    }

    /// Reads the call's request to its end, or `None` when the receiver stopped first.
    async fn read_message(
        &self,
        body: &mut h2::RecvStream,
    ) -> Result<Option<RequestMessage>, GrpcReceiverFault> {
        let mut received = Vec::new();
        let mut declared = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let chunk = nervix_primitives::select! {
                () = self.cancellation.cancelled() => return Ok(None),
                chunk = body.data() => chunk,
            };
            let chunk = match chunk {
                None => break,
                Some(Ok(chunk)) => chunk,
                Some(Err(source)) => {
                    return Err(GrpcReceiverFault::Request {
                        connection: self.connection,
                        source: StdArc::new(source),
                    });
                }
            };
            body.flow_control()
                .release_capacity(chunk.len())
                .map_err(|source| GrpcReceiverFault::Request {
                    connection: self.connection,
                    source: StdArc::new(source),
                })?;
            received.extend_from_slice(&chunk);
            if declared.is_none() && received.len() >= MESSAGE_PREFIX_BYTES {
                let length = Self::declared_length(&received);
                if length > MAX_REQUEST_MESSAGE_BYTES {
                    return Err(GrpcReceiverFault::MessageTooLarge {
                        connection: self.connection,
                        limit: MAX_REQUEST_MESSAGE_BYTES,
                    });
                }
                declared = Some(length);
            }
            if let Some(length) = declared
                && received.len()
                    > length
                        .checked_add(MESSAGE_PREFIX_BYTES)
                        .assured("a declared length is at most the message limit")
            {
                return Err(self.malformed(MessageFraming::TrailingBytes));
            }
        }
        let Some(length) = declared else {
            return Err(self.malformed(MessageFraming::TruncatedPrefix));
        };
        let message = received.split_off(MESSAGE_PREFIX_BYTES);
        if message.len() != length {
            return Err(self.malformed(MessageFraming::TruncatedMessage { declared: length }));
        }
        let compressed = match received.first() {
            Some(0) => false,
            Some(1) => true,
            Some(flag) => return Err(self.malformed(MessageFraming::UnknownFlag { flag: *flag })),
            None => return Err(self.malformed(MessageFraming::TruncatedPrefix)),
        };
        Ok(Some(RequestMessage {
            compressed,
            bytes: message,
        }))
    }

    /// The length a message prefix declares.
    fn declared_length(received: &[u8]) -> usize {
        let declared = received
            .get(1..MESSAGE_PREFIX_BYTES)
            .verified("the caller reads a length only once the whole prefix has arrived");
        let mut length = [0_u8; 4];
        length.copy_from_slice(declared);
        usize::try_from(u32::from_be_bytes(length))
            .assured("Nervix runs on 64-bit targets, so usize holds every u32")
    }

    fn malformed(&self, framing: MessageFraming) -> GrpcReceiverFault {
        GrpcReceiverFault::MalformedMessage {
            connection: self.connection,
            framing,
        }
    }

    /// Answers with an empty response message and `grpc-status: 0`.
    fn accept(&self, respond: &mut h2::server::SendResponse<Bytes>) {
        let response = Self::response_head(Vec::new());
        // A client that reset the call no longer reads any part of its answer.
        let Ok(mut stream) = respond.send_response(response, false) else {
            return;
        };
        let empty_message = Bytes::from_static(&[0; MESSAGE_PREFIX_BYTES]);
        if stream.send_data(empty_message, false).is_err() {
            return;
        }
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        stream
            .send_trailers(trailers)
            .discarded("a client that reset the call no longer reads its trailers");
    }

    /// Answers with a trailers-only response carrying `code`.
    fn respond_with_status(
        &self,
        respond: &mut h2::server::SendResponse<Bytes>,
        code: tonic::Code,
    ) {
        let status = i32::from(code).to_string();
        let status = http::HeaderValue::from_str(&status)
            .assured("a decimal status code is a valid header value");
        let response = Self::response_head(vec![
            ("grpc-status", status),
            (
                "grpc-message",
                http::HeaderValue::from_static("scripted%20by%20the%20test%20receiver"),
            ),
        ]);
        respond
            .send_response(response, true)
            .discarded("a client that reset the call no longer reads its answer");
    }

    fn response_head(headers: Vec<(&'static str, http::HeaderValue)>) -> http::Response<()> {
        let mut response = http::Response::new(());
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/grpc"),
        );
        for (name, value) in headers {
            response.headers_mut().insert(name, value);
        }
        response
    }
}

/// The one message a call's request carried.
struct RequestMessage {
    compressed: bool,
    bytes: Vec<u8>,
}
