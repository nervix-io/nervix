//! An HTTP/1.1 receiver that stands in for an endpoint an operator provisioned, so a scenario can
//! see exactly what Nervix sent it and decide exactly how it answers.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it accepts, the scripted responses they
//!   answer with, the requests they captured, the faults they observed, its own TLS identity and the
//!   client identity it issues, and the bounded stop that ends all of them.
//! - **Depends on.** Tokio, Rustls and rcgen, and the HTTP/1.1 request grammar through `httparse`.
//! - **Must not know.** Scenario state, nodes, NSPL, or what the requests it captures mean.
//!
//! # Scripted responses
//!
//! Every request is captured in full before it is answered, and each one takes the next response
//! from the script. Once the script is empty, requests take the receiver's standing response, which
//! is a complete `200` without a body until a scenario replaces it. A response can complete
//! normally, answer after a delay, precede its final response with an interim one, declare more body
//! than it sends and then stall, never answer, close the connection without answering, or write
//! arbitrary bytes. The last three are how a scenario loses a response the endpoint already acted
//! on, holds an attempt past its timeout, and sends framing no valid endpoint would.
//!
//! # Bounds
//!
//! The receiver runs inside the scenario that started it, so everything it holds is bounded. A
//! request head, a request body, the number of captured requests, and the number of recorded faults
//! each have a limit, and exceeding one is recorded as a fault rather than captured. Every wait a
//! connection makes observes the receiver's cancellation, so a held response or a stalled body ends
//! when the receiver stops. Stopping gives connections [`RECEIVER_CONNECTION_STOP_BUDGET`] to end
//! before it aborts and joins the ones that remain, and reports how many it had to force.

use std::{
    collections::VecDeque,
    fmt, io,
    net::SocketAddr,
    num::ParseIntError,
    path::Path,
    str::FromStr,
    sync::Arc as StdArc,
    time::{Duration, Instant},
};

use meticulous::{OptionExt as _, ResultExt as _};
use parking_lot::Mutex;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rustls::{
    RootCertStore, ServerConfig,
    crypto::{CryptoProvider, aws_lc_rs},
    server::WebPkiClientVerifier,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
use tempfile::TempDir;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use triomphe::Arc;

/// How long stopping waits for connections to end on their own before it aborts and joins them,
/// in seconds. Every await a connection makes also waits for the receiver's cancellation, so the
/// only thing this waits for is the scheduler reaching each connection once. A policy input.
const RECEIVER_CONNECTION_STOP_SECONDS: u64 = 5;
/// How long joining the accept loop may take once its connections are accounted for, in seconds.
/// A policy input: the loop does nothing after its connections end but return their summary.
const RECEIVER_ACCEPT_LOOP_JOIN_SECONDS: u64 = 1;
pub(crate) const RECEIVER_CONNECTION_STOP_BUDGET: Duration =
    Duration::from_secs(RECEIVER_CONNECTION_STOP_SECONDS);
/// The whole stop, connections and accept loop together.
pub(crate) const RECEIVER_STOP_BUDGET: Duration =
    Duration::from_secs(RECEIVER_CONNECTION_STOP_SECONDS + RECEIVER_ACCEPT_LOOP_JOIN_SECONDS);

/// The application header bytes one HTTP emitter request may carry.
const HTTP_EMITTER_HEADER_BYTES: usize = 32 * 1024;
/// The encoded request target one HTTP emitter request may carry.
const HTTP_EMITTER_TARGET_BYTES: usize = 8 * 1024;
/// The application headers one HTTP emitter request may carry.
const HTTP_EMITTER_HEADER_COUNT: usize = 128;
/// The largest request head the receiver reads. A policy input: large enough that a request at
/// every HTTP emitter limit at once, with its transport framing, is captured rather than refused.
pub(crate) const MAX_REQUEST_HEAD_BYTES: usize = 256 * 1024;
/// The most header fields one request head may carry. A policy input, with the same margin.
pub(crate) const MAX_REQUEST_HEADERS: usize = 512;
/// The largest request body the receiver reads. A policy input.
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
/// The most requests one receiver captures. A policy input.
pub(crate) const MAX_CAPTURED_REQUESTS: usize = 4096;
/// The most faults one receiver keeps. Faults beyond it are counted but not kept. A policy input.
pub(crate) const MAX_RECORDED_FAULTS: usize = 256;
/// The bytes one read takes from a connection.
const READ_CHUNK_BYTES: usize = 16 * 1024;

const _: () = assert!(
    HTTP_EMITTER_HEADER_BYTES + HTTP_EMITTER_TARGET_BYTES < MAX_REQUEST_HEAD_BYTES,
    "a request at every HTTP emitter limit at once must fit the receiver's request head"
);
const _: () = assert!(
    HTTP_EMITTER_HEADER_COUNT < MAX_REQUEST_HEADERS,
    "a request at the HTTP emitter header limit must leave room for transport framing fields"
);
const _: () = assert!(
    RECEIVER_CONNECTION_STOP_BUDGET.as_nanos() < RECEIVER_STOP_BUDGET.as_nanos(),
    "the whole stop must outlast the connections it waits for"
);

/// What a receiver serves its connections over.
#[derive(Clone, Debug)]
pub(crate) enum ReceiverTransport {
    Plain,
    Tls(ReceiverTlsOptions),
}

/// The certificate a TLS receiver presents and whether it demands one back.
#[derive(Clone, Debug)]
pub(crate) struct ReceiverTlsOptions {
    /// The DNS names and IP addresses the receiver's certificate is valid for. A client that dials
    /// any other name fails hostname verification.
    pub(crate) certificate_hosts: Vec<String>,
    pub(crate) client_certificate: ClientCertificatePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientCertificatePolicy {
    NotRequested,
    /// The handshake fails unless the client presents the certificate the receiver issued.
    Required,
}

/// One scripted answer to one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReceiverResponse {
    Respond(ScriptedResponse),
    /// Capture the request, then close the connection without writing anything.
    LoseResponse,
    /// Capture the request, then write nothing until the client leaves or the receiver stops.
    HoldResponse,
    /// Capture the request, write these bytes, then close the connection.
    Raw(Vec<u8>),
}

/// A response written as HTTP/1.1 framing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScriptedResponse {
    status: u16,
    headers: Vec<ResponseHeader>,
    body: Vec<u8>,
    interim: Option<u16>,
    delay: Option<Duration>,
    body_delivery: BodyDelivery,
    extra_headers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResponseHeader {
    name: String,
    value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyDelivery {
    Complete,
    /// Declare one byte more than the body holds, send the body, then stall.
    Stalled,
}

impl ScriptedResponse {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            interim: None,
            delay: None,
            body_delivery: BodyDelivery::Complete,
            extra_headers: 0,
        }
    }

    /// Statuses whose responses carry no content and so no `Content-Length`.
    fn carries_content(&self) -> bool {
        let informational = (100..200).contains(&self.status);
        !(informational || self.status == 204 || self.status == 304)
    }

    fn head(&self) -> Vec<u8> {
        let mut head = status_line(self.status).into_bytes();
        for header in &self.headers {
            head.extend_from_slice(format!("{}: {}\r\n", header.name, header.value).as_bytes());
        }
        for index in 0..self.extra_headers {
            head.extend_from_slice(format!("x-fixture-extra-{index}: extra\r\n").as_bytes());
        }
        if self.carries_content() {
            let declared = match self.body_delivery {
                BodyDelivery::Complete => self.body.len(),
                BodyDelivery::Stalled => self
                    .body
                    .len()
                    .checked_add(1)
                    .assured("a scripted body is far shorter than the address space"),
            };
            head.extend_from_slice(format!("content-length: {declared}\r\n").as_bytes());
        }
        head.extend_from_slice(b"\r\n");
        head
    }

    fn parse_clause(&mut self, clause: &str) -> Result<(), ReceiverScriptError> {
        if let Some(header) = clause.strip_prefix("header ") {
            let Some((name, value)) = header.split_once(':') else {
                return Err(ReceiverScriptError::Header {
                    clause: clause.to_string(),
                });
            };
            self.headers.push(ResponseHeader {
                name: name.trim().to_string(),
                value: value.trim().to_string(),
            });
            return Ok(());
        }
        if let Some(body) = clause.strip_prefix("body ") {
            self.body = body.as_bytes().to_vec();
            return Ok(());
        }
        if let Some(status) = clause.strip_prefix("interim ") {
            self.interim = Some(parse_status(status)?);
            return Ok(());
        }
        if let Some(delay) = clause.strip_prefix("after ") {
            let delay = humantime::parse_duration(delay).map_err(|source| {
                ReceiverScriptError::Duration {
                    text: delay.to_string(),
                    source,
                }
            })?;
            self.delay = Some(delay);
            return Ok(());
        }
        if clause == "stall body" {
            self.body_delivery = BodyDelivery::Stalled;
            return Ok(());
        }
        if let Some(count) = clause.strip_prefix("extra headers ") {
            self.extra_headers = count.parse().map_err(|source| ReceiverScriptError::Count {
                text: count.to_string(),
                source,
            })?;
            return Ok(());
        }
        Err(ReceiverScriptError::UnknownClause {
            clause: clause.to_string(),
        })
    }
}

fn status_line(status: u16) -> String {
    let reason = match http::StatusCode::from_u16(status) {
        Ok(code) => code.canonical_reason().unwrap_or("Fixture"),
        Err(_) => "Fixture",
    };
    format!("HTTP/1.1 {status} {reason}\r\n")
}

fn parse_status(text: &str) -> Result<u16, ReceiverScriptError> {
    let invalid = || ReceiverScriptError::Status {
        text: text.to_string(),
    };
    if text.len() != 3 {
        return Err(invalid());
    }
    let status = text.parse::<u16>().map_err(|_| invalid())?;
    if status < 100 {
        return Err(invalid());
    }
    Ok(status)
}

/// A script line that does not describe a response.
#[derive(Debug, Error)]
pub(crate) enum ReceiverScriptError {
    #[error(
        "receiver script line {line:?} is not one of `respond <status>`, `lose response`, `hold \
         response`, or `raw <bytes>`"
    )]
    UnknownForm { line: String },
    #[error("{text:?} is not a three-digit HTTP status")]
    Status { text: String },
    #[error(
        "response clause {clause:?} is not one of `header <name>: <value>`, `body <text>`, \
         `interim <status>`, `after <duration>`, `stall body`, or `extra headers <count>`"
    )]
    UnknownClause { clause: String },
    #[error("header clause {clause:?} has no `:` between its name and value")]
    Header { clause: String },
    #[error("{text:?} is not a duration")]
    Duration {
        text: String,
        #[source]
        source: humantime::DurationError,
    },
    #[error("{text:?} is not a header count")]
    Count {
        text: String,
        #[source]
        source: ParseIntError,
    },
    #[error("raw bytes {text:?} end inside an escape; use `\\r`, `\\n`, or `\\\\`")]
    Escape { text: String },
}

impl FromStr for ReceiverResponse {
    type Err = ReceiverScriptError;

    /// Reads one script line: `respond <status>` followed by `;`-separated clauses,
    /// `lose response`, `hold response`, or `raw <bytes>` with `\r`, `\n`, and `\\` escapes.
    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let line = line.trim();
        if line == "lose response" {
            return Ok(Self::LoseResponse);
        }
        if line == "hold response" {
            return Ok(Self::HoldResponse);
        }
        if let Some(raw) = line.strip_prefix("raw ") {
            return Ok(Self::Raw(unescape_raw(raw)?));
        }
        let Some(respond) = line.strip_prefix("respond ") else {
            return Err(ReceiverScriptError::UnknownForm {
                line: line.to_string(),
            });
        };
        let mut clauses = respond.split(';').map(str::trim);
        let status = clauses.next().unwrap_or_default();
        let mut response = ScriptedResponse::status(parse_status(status)?);
        for clause in clauses {
            response.parse_clause(clause)?;
        }
        Ok(Self::Respond(response))
    }
}

fn unescape_raw(text: &str) -> Result<Vec<u8>, ReceiverScriptError> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            let mut encoded = [0_u8; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            continue;
        }
        match characters.next() {
            Some('r') => bytes.push(b'\r'),
            Some('n') => bytes.push(b'\n'),
            Some('\\') => bytes.push(b'\\'),
            Some(_) | None => {
                return Err(ReceiverScriptError::Escape {
                    text: text.to_string(),
                });
            }
        }
    }
    Ok(bytes)
}

/// One request exactly as the receiver read it.
#[derive(Clone, Debug)]
pub(crate) struct CapturedRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    headers: Vec<CapturedHeader>,
    pub(crate) body: Vec<u8>,
}

#[derive(Clone, Debug)]
struct CapturedHeader {
    name: String,
    value: Vec<u8>,
}

impl CapturedRequest {
    /// Every value sent under `name`, compared without ASCII case, in the order they arrived.
    pub(crate) fn header_values(&self, name: &str) -> Vec<&[u8]> {
        self.headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_slice())
            .collect()
    }
}

impl fmt::Display for CapturedRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "{} {}", self.method, self.target)?;
        for header in &self.headers {
            writeln!(
                formatter,
                "{}: {}",
                header.name,
                String::from_utf8_lossy(&header.value)
            )?;
        }
        writeln!(formatter)?;
        write!(formatter, "{}", String::from_utf8_lossy(&self.body))
    }
}

/// Something a connection did that was not a request the receiver could capture.
#[derive(Clone, Debug, Error)]
pub(crate) enum ReceiverFault {
    #[error("connection {connection} sent a request head larger than {limit} bytes")]
    RequestHeadTooLarge { connection: u64, limit: usize },
    #[error("connection {connection} sent a request body larger than {limit} bytes")]
    RequestBodyTooLarge { connection: u64, limit: usize },
    #[error("connection {connection} sent a malformed request head: {reason}")]
    MalformedRequest {
        connection: u64,
        reason: httparse::Error,
    },
    #[error("connection {connection} sent a malformed chunked body")]
    MalformedChunk { connection: u64 },
    #[error("connection {connection} sent a Content-Length that is not a length")]
    MalformedContentLength { connection: u64 },
    #[error("connection {connection} closed in the middle of a request")]
    IncompleteRequest { connection: u64 },
    #[error("connection {connection} failed its TLS handshake")]
    TlsHandshake {
        connection: u64,
        #[source]
        source: StdArc<io::Error>,
    },
    #[error("connection {connection} failed")]
    Connection {
        connection: u64,
        #[source]
        source: StdArc<io::Error>,
    },
    #[error("accepting a connection failed")]
    Accept {
        #[source]
        source: StdArc<io::Error>,
    },
    #[error(
        "connection {connection} sent a request after the receiver captured its limit of {limit}"
    )]
    CaptureLimit { connection: u64, limit: usize },
}

impl ReceiverFault {
    pub(crate) fn is_tls_handshake(&self) -> bool {
        matches!(self, Self::TlsHandshake { .. })
    }
}

/// A receiver could not start.
#[derive(Debug, Error)]
pub(crate) enum ReceiverStartError {
    #[error("binding the receiver to {address} failed")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("generating the receiver's TLS identity failed")]
    Certificate(#[source] rcgen::Error),
    #[error("writing the receiver's TLS files failed")]
    TlsFiles(#[source] io::Error),
    #[error("configuring the receiver's TLS server failed")]
    TlsConfiguration(#[source] rustls::Error),
    #[error("configuring the receiver's client certificate verifier failed")]
    ClientVerifier(#[source] rustls::server::VerifierBuilderError),
}

/// A wait on a receiver ended at its deadline.
#[derive(Debug, Error)]
pub(crate) enum ReceiverWaitError {
    #[error(
        "the receiver captured {captured} of the {expected} requests expected within {waited:?}; \
         {faults} fault(s), the latest: {latest_fault}"
    )]
    Requests {
        expected: usize,
        captured: usize,
        waited: Duration,
        faults: usize,
        latest_fault: LatestFault,
    },
    #[error(
        "the receiver recorded no {description} within {waited:?}; it captured {captured} \
         request(s) and {faults} fault(s), the latest: {latest_fault}"
    )]
    Fault {
        description: &'static str,
        captured: usize,
        waited: Duration,
        faults: usize,
        latest_fault: LatestFault,
    },
}

/// The most recent fault a failed wait reports, or that there was none.
#[derive(Debug)]
pub(crate) enum LatestFault {
    None,
    Fault(ReceiverFault),
}

impl fmt::Display for LatestFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(formatter, "none"),
            Self::Fault(fault) => write!(formatter, "{fault}"),
        }
    }
}

/// The client side of a TLS receiver's identity, written where a node can mount it.
#[derive(Debug)]
struct ReceiverTlsFiles {
    directory: TempDir,
}

impl ReceiverTlsFiles {
    const CA: &'static str = "ca.pem";
    const CLIENT_CERTIFICATE: &'static str = "client.pem";
    const CLIENT_KEY: &'static str = "client-key.pem";
    const FILES: [&'static str; 3] = [Self::CA, Self::CLIENT_CERTIFICATE, Self::CLIENT_KEY];
}

struct ReceiverTlsIdentity {
    acceptor: TlsAcceptor,
    files: ReceiverTlsFiles,
}

impl ReceiverTlsIdentity {
    fn generate(options: &ReceiverTlsOptions) -> Result<Self, ReceiverStartError> {
        let ca_key = KeyPair::generate().map_err(ReceiverStartError::Certificate)?;
        let mut ca_params =
            CertificateParams::new(Vec::new()).map_err(ReceiverStartError::Certificate)?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "nervix http receiver ca");
        let ca_certificate = ca_params
            .self_signed(&ca_key)
            .map_err(ReceiverStartError::Certificate)?;

        let server_key = KeyPair::generate().map_err(ReceiverStartError::Certificate)?;
        let server_params = CertificateParams::new(options.certificate_hosts.clone())
            .map_err(ReceiverStartError::Certificate)?;
        let server_certificate = server_params
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .map_err(ReceiverStartError::Certificate)?;

        let client_key = KeyPair::generate().map_err(ReceiverStartError::Certificate)?;
        let mut client_params =
            CertificateParams::new(Vec::new()).map_err(ReceiverStartError::Certificate)?;
        client_params
            .distinguished_name
            .push(DnType::CommonName, "nervix http receiver client");
        let client_certificate = client_params
            .signed_by(&client_key, &ca_certificate, &ca_key)
            .map_err(ReceiverStartError::Certificate)?;

        let directory = tempfile::tempdir().map_err(ReceiverStartError::TlsFiles)?;
        let write = |name: &str, contents: String| {
            std::fs::write(directory.path().join(name), contents)
                .map_err(ReceiverStartError::TlsFiles)
        };
        write(ReceiverTlsFiles::CA, ca_certificate.pem())?;
        write(
            ReceiverTlsFiles::CLIENT_CERTIFICATE,
            client_certificate.pem(),
        )?;
        write(ReceiverTlsFiles::CLIENT_KEY, client_key.serialize_pem())?;

        let provider = StdArc::new(aws_lc_rs::default_provider());
        let builder = ServerConfig::builder_with_provider(StdArc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(ReceiverStartError::TlsConfiguration)?;
        let builder = match options.client_certificate {
            ClientCertificatePolicy::NotRequested => builder.with_no_client_auth(),
            ClientCertificatePolicy::Required => {
                let verifier = Self::client_verifier(ca_certificate.der().clone(), provider)?;
                builder.with_client_cert_verifier(verifier)
            }
        };
        let server_key = PrivateKeyDer::from_pem_slice(server_key.serialize_pem().as_bytes())
            .assured("rcgen serializes the key it generated as valid PEM");
        let mut config = builder
            .with_single_cert(vec![server_certificate.der().clone()], server_key)
            .map_err(ReceiverStartError::TlsConfiguration)?;
        // The receiver speaks HTTP/1.1 only, so a client that offers HTTP/2 falls back to it.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            acceptor: TlsAcceptor::from(StdArc::new(config)),
            files: ReceiverTlsFiles { directory },
        })
    }

    fn client_verifier(
        ca: CertificateDer<'static>,
        provider: StdArc<CryptoProvider>,
    ) -> Result<StdArc<dyn rustls::server::danger::ClientCertVerifier>, ReceiverStartError> {
        let mut roots = RootCertStore::empty();
        roots
            .add(ca)
            .map_err(ReceiverStartError::TlsConfiguration)?;
        WebPkiClientVerifier::builder_with_provider(StdArc::new(roots), provider)
            .build()
            .map_err(ReceiverStartError::ClientVerifier)
    }
}

/// What every connection of one receiver shares.
struct ReceiverState {
    script: Mutex<ReceiverScript>,
    captured: Mutex<Vec<CapturedRequest>>,
    faults: Mutex<RecordedFaults>,
    captured_count: watch::Sender<usize>,
    fault_count: watch::Sender<usize>,
}

struct ReceiverScript {
    pending: VecDeque<ReceiverResponse>,
    standing: ReceiverResponse,
}

#[derive(Default)]
struct RecordedFaults {
    kept: Vec<ReceiverFault>,
    total: usize,
}

/// Whether the receiver kept a request or had already captured its limit.
enum Capture {
    Kept,
    AtLimit,
}

impl ReceiverState {
    fn new() -> Self {
        Self {
            script: Mutex::new(ReceiverScript {
                pending: VecDeque::new(),
                standing: ReceiverResponse::Respond(ScriptedResponse::status(200)),
            }),
            captured: Mutex::new(Vec::new()),
            faults: Mutex::new(RecordedFaults::default()),
            captured_count: watch::Sender::new(0),
            fault_count: watch::Sender::new(0),
        }
    }

    fn next_response(&self) -> ReceiverResponse {
        let mut script = self.script.lock();
        match script.pending.pop_front() {
            Some(response) => response,
            None => script.standing.clone(),
        }
    }

    fn capture(&self, request: CapturedRequest) -> Capture {
        // The capture lock is released before the count is published, for the same reason as a
        // fault's: a request wait reads the captures while it can still hold the count's read lock.
        let count = {
            let mut captured = self.captured.lock();
            if captured.len() >= MAX_CAPTURED_REQUESTS {
                return Capture::AtLimit;
            }
            captured.push(request);
            captured.len()
        };
        self.captured_count.send_replace(count);
        Capture::Kept
    }

    fn record(&self, fault: ReceiverFault) {
        // The fault lock is released before the count is published: a fault wait evaluates its
        // predicate, which takes the fault lock, while it holds the count's read lock.
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

    fn latest_fault(&self) -> LatestFault {
        match self.faults.lock().kept.last() {
            Some(fault) => LatestFault::Fault(fault.clone()),
            None => LatestFault::None,
        }
    }

    fn fault_total(&self) -> usize {
        self.faults.lock().total
    }

    fn has_fault(&self, expected: fn(&ReceiverFault) -> bool) -> bool {
        self.faults.lock().kept.iter().any(expected)
    }
}

/// A running receiver. Dropping it aborts every task it started; [`HttpReceiver::stop`] ends them
/// within [`RECEIVER_STOP_BUDGET`] and reports how.
pub(crate) struct HttpReceiver {
    address: SocketAddr,
    tls_files: Option<ReceiverTlsFiles>,
    state: Arc<ReceiverState>,
    cancellation: CancellationToken,
    accept_loop: AbortOnDropHandle<ConnectionSummary>,
}

impl fmt::Debug for HttpReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpReceiver")
            .field("address", &self.address)
            .field("tls", &self.tls_files.is_some())
            .field("captured", &*self.state.captured_count.borrow())
            .field("faults", &*self.state.fault_count.borrow())
            .finish()
    }
}

impl HttpReceiver {
    pub(crate) async fn start(
        address: SocketAddr,
        transport: ReceiverTransport,
    ) -> Result<Self, ReceiverStartError> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| ReceiverStartError::Bind { address, source })?;
        let address = listener
            .local_addr()
            .map_err(|source| ReceiverStartError::Bind { address, source })?;
        let identity = match &transport {
            ReceiverTransport::Plain => None,
            ReceiverTransport::Tls(options) => Some(ReceiverTlsIdentity::generate(options)?),
        };
        let (acceptor, tls_files) = match identity {
            Some(identity) => (Some(identity.acceptor), Some(identity.files)),
            None => (None, None),
        };
        let state = Arc::new(ReceiverState::new());
        let cancellation = CancellationToken::new();
        let accept_loop = AcceptLoop {
            listener,
            acceptor,
            state: state.clone(),
            cancellation: cancellation.clone(),
        };
        Ok(Self {
            address,
            tls_files,
            state,
            cancellation,
            accept_loop: AbortOnDropHandle::new(tokio::spawn(accept_loop.run())),
        })
    }

    /// The origin a client dials, `http://127.0.0.1:<port>` or `https://127.0.0.1:<port>`.
    pub(crate) fn origin(&self) -> String {
        let scheme = match self.tls_files {
            Some(_) => "https",
            None => "http",
        };
        format!("{scheme}://{}", self.address)
    }

    pub(crate) fn port(&self) -> u16 {
        self.address.port()
    }

    /// The directory holding `ca.pem`, `client.pem`, and `client-key.pem`, for a TLS receiver.
    pub(crate) fn tls_files(&self) -> Option<&Path> {
        match &self.tls_files {
            Some(files) => Some(files.directory.path()),
            None => None,
        }
    }

    /// The names of the files [`HttpReceiver::tls_files`] holds.
    pub(crate) fn tls_file_names() -> [&'static str; 3] {
        ReceiverTlsFiles::FILES
    }

    /// Appends responses the next requests take, in order.
    pub(crate) fn script(&self, responses: impl IntoIterator<Item = ReceiverResponse>) {
        self.state.script.lock().pending.extend(responses);
    }

    /// Replaces the response every request takes once the script is empty.
    pub(crate) fn answer_unscripted_requests_with(&self, response: ReceiverResponse) {
        self.state.script.lock().standing = response;
    }

    pub(crate) fn captured(&self) -> Vec<CapturedRequest> {
        self.state.captured.lock().clone()
    }

    /// Waits until the receiver has captured at least `expected` requests.
    pub(crate) async fn wait_for_requests(
        &self,
        expected: usize,
        within: Duration,
    ) -> Result<Vec<CapturedRequest>, ReceiverWaitError> {
        let mut captured_count = self.state.captured_count.subscribe();
        let waited =
            tokio::time::timeout(within, captured_count.wait_for(|count| *count >= expected)).await;
        match waited {
            Ok(Ok(_)) => Ok(self.captured()),
            Ok(Err(_)) | Err(_) => Err(ReceiverWaitError::Requests {
                expected,
                captured: *self.state.captured_count.borrow(),
                waited: within,
                faults: self.state.fault_total(),
                latest_fault: self.state.latest_fault(),
            }),
        }
    }

    /// Waits until the receiver has recorded a fault that `expected` accepts. `description` names
    /// the fault in the failure.
    pub(crate) async fn wait_for_fault(
        &self,
        description: &'static str,
        expected: fn(&ReceiverFault) -> bool,
        within: Duration,
    ) -> Result<(), ReceiverWaitError> {
        let mut fault_count = self.state.fault_count.subscribe();
        let state = self.state.clone();
        let waited =
            tokio::time::timeout(within, fault_count.wait_for(|_| state.has_fault(expected))).await;
        match waited {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(ReceiverWaitError::Fault {
                description,
                captured: *self.state.captured_count.borrow(),
                waited: within,
                faults: self.state.fault_total(),
                latest_fault: self.state.latest_fault(),
            }),
        }
    }

    /// Stops accepting, ends every connection, and reports how the stop went.
    pub(crate) async fn stop(self) -> HttpReceiverStop {
        let started = Instant::now();
        self.cancellation.cancel();
        let Self {
            state,
            mut accept_loop,
            ..
        } = self;
        let joined = tokio::time::timeout(RECEIVER_STOP_BUDGET, &mut accept_loop).await;
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
        HttpReceiverStop {
            elapsed: started.elapsed(),
            captured: *state.captured_count.borrow(),
            faults: state.fault_total(),
            ending,
        }
    }
}

/// How one receiver's stop went.
#[derive(Debug)]
pub(crate) struct HttpReceiverStop {
    pub(crate) elapsed: Duration,
    pub(crate) captured: usize,
    pub(crate) faults: usize,
    pub(crate) ending: AcceptLoopEnding,
}

#[derive(Debug)]
pub(crate) enum AcceptLoopEnding {
    /// The accept loop returned the summary of every connection it had started.
    Returned(ConnectionSummary),
    /// The accept loop outlived the stop budget and was aborted and joined.
    Aborted,
    /// The accept loop panicked.
    Failed(tokio::task::JoinError),
}

impl HttpReceiverStop {
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

impl fmt::Display for HttpReceiverStop {
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
            "; captured {} request(s), recorded {} fault(s)",
            self.captured, self.faults
        )
    }
}

/// How the connections of one receiver ended.
#[derive(Debug, Default)]
pub(crate) struct ConnectionSummary {
    pub(crate) served: usize,
    pub(crate) forced: usize,
    pub(crate) panicked: usize,
}

impl ConnectionSummary {
    fn joined(&mut self, joined: Result<(), tokio::task::JoinError>) {
        match joined {
            Ok(()) => {}
            Err(error) if error.is_panic() => {
                self.panicked = checked_increment(self.panicked);
            }
            Err(_) => self.forced = checked_increment(self.forced),
        }
    }
}

fn checked_increment(count: usize) -> usize {
    count
        .checked_add(1)
        .assured("one receiver's connections are bounded by the sockets a process can open")
}

struct AcceptLoop {
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    state: Arc<ReceiverState>,
    cancellation: CancellationToken,
}

impl AcceptLoop {
    async fn run(self) -> ConnectionSummary {
        let mut connections = JoinSet::new();
        let mut summary = ConnectionSummary::default();
        let mut next_connection = 0_u64;
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    summary.joined(joined);
                }
                accepted = self.listener.accept() => {
                    let stream = match accepted {
                        Ok((stream, _)) => stream,
                        Err(source) => {
                            self.state.record(ReceiverFault::Accept {
                                source: StdArc::new(source),
                            });
                            continue;
                        }
                    };
                    let connection = Connection {
                        id: next_connection,
                        state: self.state.clone(),
                        cancellation: self.cancellation.clone(),
                    };
                    next_connection = next_connection
                        .checked_add(1)
                        .assured("a receiver cannot accept 2^64 connections");
                    summary.served = checked_increment(summary.served);
                    connections.spawn(connection.serve(stream, self.acceptor.clone()));
                }
            }
        }
        let deadline = tokio::time::Instant::now() + RECEIVER_CONNECTION_STOP_BUDGET;
        loop {
            tokio::task::consume_budget().await;
            match tokio::time::timeout_at(deadline, connections.join_next()).await {
                Ok(Some(joined)) => summary.joined(joined),
                Ok(None) => break,
                Err(_) => {
                    connections.abort_all();
                    while let Some(joined) = connections.join_next().await {
                        tokio::task::consume_budget().await;
                        summary.joined(joined);
                    }
                    break;
                }
            }
        }
        summary
    }
}

/// Why one connection stopped serving requests.
enum ConnectionEnd {
    /// The client closed between requests, or the receiver stopped.
    Closed,
    /// The connection did something the receiver recorded as a fault.
    Faulted(ReceiverFault),
}

struct Connection {
    id: u64,
    state: Arc<ReceiverState>,
    cancellation: CancellationToken,
}

impl Connection {
    async fn serve(self, stream: TcpStream, acceptor: Option<TlsAcceptor>) {
        let ending = match acceptor {
            None => self.serve_requests(stream).await,
            Some(acceptor) => {
                let accepted = tokio::select! {
                    () = self.cancellation.cancelled() => return,
                    accepted = acceptor.accept(stream) => accepted,
                };
                match accepted {
                    Ok(stream) => self.serve_requests(stream).await,
                    Err(source) => ConnectionEnd::Faulted(ReceiverFault::TlsHandshake {
                        connection: self.id,
                        source: StdArc::new(source),
                    }),
                }
            }
        };
        match ending {
            ConnectionEnd::Closed => {}
            ConnectionEnd::Faulted(fault) => self.state.record(fault),
        }
    }

    async fn serve_requests<S>(&self, mut stream: S) -> ConnectionEnd
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut buffer = Vec::new();
        loop {
            tokio::task::consume_budget().await;
            let request = match self.read_request(&mut stream, &mut buffer).await {
                Ok(Some(request)) => request,
                Ok(None) => return ConnectionEnd::Closed,
                Err(fault) => return ConnectionEnd::Faulted(fault),
            };
            let closes = request.closes;
            match self.state.capture(request.captured) {
                Capture::Kept => {}
                Capture::AtLimit => {
                    return ConnectionEnd::Faulted(ReceiverFault::CaptureLimit {
                        connection: self.id,
                        limit: MAX_CAPTURED_REQUESTS,
                    });
                }
            }
            let answered = match self.state.next_response() {
                ReceiverResponse::Respond(response) => self.respond(&mut stream, &response).await,
                ReceiverResponse::LoseResponse => return ConnectionEnd::Closed,
                ReceiverResponse::HoldResponse => {
                    return self.hold(&mut stream).await;
                }
                ReceiverResponse::Raw(bytes) => {
                    return match self.write(&mut stream, &bytes).await {
                        Ok(Answered::Continue | Answered::Hold) => ConnectionEnd::Closed,
                        Err(end) => end,
                    };
                }
            };
            match answered {
                Ok(Answered::Continue) if !closes => {}
                Ok(Answered::Continue) => return ConnectionEnd::Closed,
                Ok(Answered::Hold) => return self.hold(&mut stream).await,
                Err(end) => return end,
            }
        }
    }

    async fn respond<S>(
        &self,
        stream: &mut S,
        response: &ScriptedResponse,
    ) -> Result<Answered, ConnectionEnd>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if let Some(delay) = response.delay {
            tokio::select! {
                () = self.cancellation.cancelled() => return Err(ConnectionEnd::Closed),
                () = tokio::time::sleep(delay) => {}
            }
        }
        if let Some(interim) = response.interim {
            let mut interim_head = status_line(interim).into_bytes();
            interim_head.extend_from_slice(b"\r\n");
            self.write(stream, &interim_head).await?;
        }
        self.write(stream, &response.head()).await?;
        if response.carries_content() {
            self.write(stream, &response.body).await?;
        }
        if response.status == 101 {
            return Ok(Answered::Hold);
        }
        match response.body_delivery {
            BodyDelivery::Complete => Ok(Answered::Continue),
            BodyDelivery::Stalled => Ok(Answered::Hold),
        }
    }

    async fn write<S>(&self, stream: &mut S, bytes: &[u8]) -> Result<Answered, ConnectionEnd>
    where
        S: AsyncWrite + Unpin,
    {
        let written = tokio::select! {
            () = self.cancellation.cancelled() => return Err(ConnectionEnd::Closed),
            written = async {
                stream.write_all(bytes).await?;
                stream.flush().await
            } => written,
        };
        match written {
            Ok(()) => Ok(Answered::Continue),
            Err(source) => Err(self.connection_failed(source)),
        }
    }

    /// Writes nothing more, discarding whatever the client sends, until it leaves or the receiver
    /// stops.
    async fn hold<S>(&self, stream: &mut S) -> ConnectionEnd
    where
        S: AsyncRead + Unpin,
    {
        let mut discard = vec![0_u8; READ_CHUNK_BYTES];
        loop {
            tokio::task::consume_budget().await;
            let read = tokio::select! {
                () = self.cancellation.cancelled() => return ConnectionEnd::Closed,
                read = stream.read(&mut discard) => read,
            };
            match read {
                Ok(0) | Err(_) => return ConnectionEnd::Closed,
                Ok(_) => {}
            }
        }
    }

    /// Reads one complete request, or `None` when the client closed between requests or the
    /// receiver stopped.
    async fn read_request<S>(
        &self,
        stream: &mut S,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<ReadRequest>, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        let head = loop {
            tokio::task::consume_budget().await;
            if let Some(head) = self.parse_head(buffer)? {
                break head;
            }
            if buffer.len() > MAX_REQUEST_HEAD_BYTES {
                return Err(ReceiverFault::RequestHeadTooLarge {
                    connection: self.id,
                    limit: MAX_REQUEST_HEAD_BYTES,
                });
            }
            match self.fill(stream, buffer).await? {
                Filled::Read => {}
                Filled::Closed if buffer.is_empty() => return Ok(None),
                Filled::Closed => {
                    return Err(ReceiverFault::IncompleteRequest {
                        connection: self.id,
                    });
                }
                Filled::Stopped => return Ok(None),
            }
        };
        buffer.drain(..head.length);
        let body = match head.framing {
            BodyFraming::Empty => Some(Vec::new()),
            BodyFraming::Length(length) => self.read_exact_body(stream, buffer, length).await?,
            BodyFraming::Chunked => self.read_chunked_body(stream, buffer).await?,
        };
        let Some(body) = body else {
            return Ok(None);
        };
        Ok(Some(ReadRequest {
            captured: CapturedRequest {
                method: head.method,
                target: head.target,
                headers: head.headers,
                body,
            },
            closes: head.closes,
        }))
    }

    fn parse_head(&self, buffer: &[u8]) -> Result<Option<RequestHead>, ReceiverFault> {
        let mut headers = vec![httparse::EMPTY_HEADER; MAX_REQUEST_HEADERS];
        let mut request = httparse::Request::new(&mut headers);
        let parsed = request
            .parse(buffer)
            .map_err(|reason| ReceiverFault::MalformedRequest {
                connection: self.id,
                reason,
            })?;
        let httparse::Status::Complete(length) = parsed else {
            return Ok(None);
        };
        let method = request.method.unwrap_or_default().to_string();
        let target = request.path.unwrap_or_default().to_string();
        let mut captured = Vec::with_capacity(request.headers.len());
        let mut framing = BodyFraming::Empty;
        let mut closes = request.version == Some(0);
        for header in request.headers.iter() {
            let name = header.name.to_string();
            if name.eq_ignore_ascii_case("content-length") {
                framing = BodyFraming::Length(self.content_length(header.value)?);
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && header.value.eq_ignore_ascii_case(b"chunked")
            {
                framing = BodyFraming::Chunked;
            } else if name.eq_ignore_ascii_case("connection") {
                closes = header.value.eq_ignore_ascii_case(b"close");
            }
            captured.push(CapturedHeader {
                name,
                value: header.value.to_vec(),
            });
        }
        Ok(Some(RequestHead {
            length,
            method,
            target,
            headers: captured,
            framing,
            closes,
        }))
    }

    fn content_length(&self, value: &[u8]) -> Result<usize, ReceiverFault> {
        let malformed = || ReceiverFault::MalformedContentLength {
            connection: self.id,
        };
        let text = std::str::from_utf8(value).map_err(|_| malformed())?;
        let length = text.trim().parse::<usize>().map_err(|_| malformed())?;
        if length > MAX_REQUEST_BODY_BYTES {
            return Err(ReceiverFault::RequestBodyTooLarge {
                connection: self.id,
                limit: MAX_REQUEST_BODY_BYTES,
            });
        }
        Ok(length)
    }

    async fn read_exact_body<S>(
        &self,
        stream: &mut S,
        buffer: &mut Vec<u8>,
        length: usize,
    ) -> Result<Option<Vec<u8>>, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        while buffer.len() < length {
            tokio::task::consume_budget().await;
            match self.fill(stream, buffer).await? {
                Filled::Read => {}
                Filled::Closed => {
                    return Err(ReceiverFault::IncompleteRequest {
                        connection: self.id,
                    });
                }
                Filled::Stopped => return Ok(None),
            }
        }
        Ok(Some(buffer.drain(..length).collect()))
    }

    async fn read_chunked_body<S>(
        &self,
        stream: &mut S,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        let too_large = ReceiverFault::RequestBodyTooLarge {
            connection: self.id,
            limit: MAX_REQUEST_BODY_BYTES,
        };
        let mut body = Vec::new();
        loop {
            tokio::task::consume_budget().await;
            let parsed =
                httparse::parse_chunk_size(buffer).map_err(|_| ReceiverFault::MalformedChunk {
                    connection: self.id,
                })?;
            let httparse::Status::Complete((size_line, size)) = parsed else {
                if !self.fill_body(stream, buffer).await? {
                    return Ok(None);
                }
                continue;
            };
            buffer.drain(..size_line);
            if size == 0 {
                if self.read_trailers(stream, buffer).await? {
                    return Ok(Some(body));
                }
                return Ok(None);
            }
            let Ok(size) = usize::try_from(size) else {
                return Err(too_large);
            };
            let Some(total) = body.len().checked_add(size) else {
                return Err(too_large);
            };
            if total > MAX_REQUEST_BODY_BYTES {
                return Err(too_large);
            }
            let chunk_with_delimiter = size
                .checked_add(2)
                .verified("the chunk size was checked against the body limit above");
            while buffer.len() < chunk_with_delimiter {
                tokio::task::consume_budget().await;
                if !self.fill_body(stream, buffer).await? {
                    return Ok(None);
                }
            }
            let chunk: Vec<u8> = buffer.drain(..size).collect();
            let delimiter: Vec<u8> = buffer.drain(..2).collect();
            if delimiter != b"\r\n" {
                return Err(ReceiverFault::MalformedChunk {
                    connection: self.id,
                });
            }
            body.extend_from_slice(&chunk);
        }
    }

    /// Consumes the trailer section after the last chunk. Returns `false` when the receiver
    /// stopped first.
    async fn read_trailers<S>(
        &self,
        stream: &mut S,
        buffer: &mut Vec<u8>,
    ) -> Result<bool, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            tokio::task::consume_budget().await;
            if buffer.starts_with(b"\r\n") {
                buffer.drain(..2);
                return Ok(true);
            }
            if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                buffer.drain(..end);
                buffer.drain(..4);
                return Ok(true);
            }
            if buffer.len() > MAX_REQUEST_HEAD_BYTES {
                return Err(ReceiverFault::RequestHeadTooLarge {
                    connection: self.id,
                    limit: MAX_REQUEST_HEAD_BYTES,
                });
            }
            if !self.fill_body(stream, buffer).await? {
                return Ok(false);
            }
        }
    }

    /// Reads more of a body the client has started. Returns `false` when the receiver stopped.
    async fn fill_body<S>(
        &self,
        stream: &mut S,
        buffer: &mut Vec<u8>,
    ) -> Result<bool, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        match self.fill(stream, buffer).await? {
            Filled::Read => Ok(true),
            Filled::Closed => Err(ReceiverFault::IncompleteRequest {
                connection: self.id,
            }),
            Filled::Stopped => Ok(false),
        }
    }

    async fn fill<S>(&self, stream: &mut S, buffer: &mut Vec<u8>) -> Result<Filled, ReceiverFault>
    where
        S: AsyncRead + Unpin,
    {
        let mut chunk = vec![0_u8; READ_CHUNK_BYTES];
        let read = tokio::select! {
            () = self.cancellation.cancelled() => return Ok(Filled::Stopped),
            read = stream.read(&mut chunk) => read,
        };
        match read {
            Ok(0) => Ok(Filled::Closed),
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                Ok(Filled::Read)
            }
            Err(source) => Err(ReceiverFault::Connection {
                connection: self.id,
                source: StdArc::new(source),
            }),
        }
    }

    fn connection_failed(&self, source: io::Error) -> ConnectionEnd {
        ConnectionEnd::Faulted(ReceiverFault::Connection {
            connection: self.id,
            source: StdArc::new(source),
        })
    }
}

/// What a connection does after it wrote a response.
enum Answered {
    /// Read the next request.
    Continue,
    /// Write nothing more until the client leaves or the receiver stops.
    Hold,
}

enum Filled {
    Read,
    Closed,
    Stopped,
}

struct RequestHead {
    length: usize,
    method: String,
    target: String,
    headers: Vec<CapturedHeader>,
    framing: BodyFraming,
    closes: bool,
}

enum BodyFraming {
    Empty,
    Length(usize),
    Chunked,
}

struct ReadRequest {
    captured: CapturedRequest,
    /// The client asked for the connection to close after this request.
    closes: bool,
}
