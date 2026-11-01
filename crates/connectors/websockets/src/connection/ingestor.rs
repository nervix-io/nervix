//! WebSocket client ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Outbound transport requirements and configuration, DNS-backed resume within the
//!   client's `connect_timeout_ms`, the typed refusal of an upgrade answer that contradicts the
//!   request's subprotocol offer, signaling before readiness, accepted-frame storage, payload
//!   delivery, and suspend and close operations.
//! - **Depends on.** The ingestor contract, typed plans and host services, compiled signaling engine,
//!   vocabulary values, execution primitives, TLS configuration, and WebSocket transport.
//! - **Must not know.** Configuration Models, Plexus, runtime collectors, relays, branches,
//!   schedules, registry state, endpoint listeners, or host intake, admission and retry loops.
//!
//! Current boundary violation: the composer hard-codes the ingestor's buffering, flush and
//! admission requirements instead of reading a connector-owned declaration from the contract.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::Duration;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::OptionExt as _;
#[cfg(test)]
use meticulous::ResultExt as _;
use nervix_connector::{
    BrokerIngestorConnector, ConfiguredDeadline, IngestMessageHeaders, IngestMessageMetadata,
    IngestMetadataRow, IngestMetadataValues, IngestRowHeaders, IngestorBatch, IngestorBatchRequest,
    IngestorConnector, IngestorError, IngestorMessage, IngestorReadCancellation, IngestorResult,
    IngestorResume, RetainedIngestHeaders, RustlsClientConfigProvider,
};
use nervix_dns::{DnsResolver, LookupShare, OperationBudget};
use nervix_models::{
    ClientConfigEntry, ConnectorKind, WEBSOCKETS_CONTROL_FRAME_BUDGET, WebsocketsClientConfig,
    WebsocketsUpgradeHeaders,
};
use nervix_primitives::{net::TcpStream, sync::Arc, time::timeout};
use thiserror::Error;
use tokio_tungstenite::{
    Connector, client_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest as _,
        error::{ProtocolError, SubProtocolError},
        protocol::WebSocketConfig,
    },
};

use crate::{
    CompiledSignalingProtocol, SignalingDataEmitter, WebsocketDataFrame, WebsocketSignalingSession,
};

const WEBSOCKETS: ConnectorKind = ConnectorKind::Websockets;

#[derive(Debug, Error)]
enum WebsocketsConnectionIngestorFailure {
    #[error("WebSocket peer sent no frame within the {budget:?} idle read deadline")]
    IdleTimeout { budget: Duration },
    #[error("WebSocket ping could not be written within {budget:?}")]
    PingTimeout { budget: Duration },
    #[error("WebSocket peer closed with code {code}: {reason}")]
    PeerClosed { code: u16, reason: String },
    #[error("WebSocket peer closed without a status")]
    PeerClosedWithoutStatus,
    #[error("WebSocket peer ended its connection")]
    Ended,
    #[error("WebSocket close handshake failed")]
    CloseTransport,
    #[error("WebSocket close handshake did not finish within {budget:?}")]
    CloseTimeout { budget: Duration },
    #[error("WebSocket peer selected none of the subprotocols the upgrade offered")]
    SubprotocolNotSelected,
    #[error("WebSocket peer selected a subprotocol the upgrade did not offer")]
    SubprotocolNotOffered,
    #[error("WebSocket peer selected a subprotocol although the upgrade offered none")]
    SubprotocolNotRequested,
}

/// The transport fails an upgrade whose answer contradicts the request's subprotocol offer (RFC
/// 6455 section 4.1), and its refusal already distinguishes how. A refusal names no subprotocol:
/// an offer can carry a credential.
impl From<SubProtocolError> for WebsocketsConnectionIngestorFailure {
    fn from(refusal: SubProtocolError) -> Self {
        match refusal {
            SubProtocolError::NoSubProtocol => Self::SubprotocolNotSelected,
            SubProtocolError::InvalidSubProtocol => Self::SubprotocolNotOffered,
            SubProtocolError::ServerSentSubProtocolNoneRequested => Self::SubprotocolNotRequested,
        }
    }
}

#[derive(Debug, Error)]
pub enum WebsocketsConnectionIngestorPlanError {
    #[error("invalid WebSockets client configuration")]
    Configuration,
}

#[derive(Clone)]
pub struct WebsocketsConnectionIngestorPlan {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    entries: Vec<ClientConfigEntry>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    headers: WebsocketsUpgradeHeaders,
    ping_interval: Option<Duration>,
    idle_deadline: connection::WebsocketIdleDeadline,
    handshake_capacity: usize,
    transport_config: WebSocketConfig,
    /// How long opening one connection may take: name resolution, the address attempts, TLS and
    /// the upgrade handshake.
    connect_timeout: ConfiguredDeadline,
}

/// The configured request authority and the socket target it names. The request itself always
/// uses `raw`, leaving Host, SNI, path, query, and upgrade headers with the original URL.
#[derive(Clone)]
struct WebsocketEndpoint {
    raw: String,
    host: String,
    port: u16,
    requires_tls: bool,
}

impl WebsocketsConnectionIngestorPlan {
    pub fn new(
        entries: Vec<ClientConfigEntry>,
        headers: WebsocketsUpgradeHeaders,
        signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
        dns: DnsResolver,
    ) -> Result<Self, Report<WebsocketsConnectionIngestorPlanError>> {
        let config = WebsocketsClientConfig::parse(&entries)
            .change_context(WebsocketsConnectionIngestorPlanError::Configuration)?;
        let connect_timeout = ConfiguredDeadline::connect(&entries)
            .change_context(WebsocketsConnectionIngestorPlanError::Configuration)?;
        let parsed = config.endpoint();
        let endpoint_requires_tls = parsed.scheme() == "wss";
        let host = parsed
            .host_str()
            .verified("the client schema requires an endpoint host");
        let port = parsed
            .port_or_known_default()
            .verified("ws and wss have known default ports, and explicit ports were parsed");
        let idle_deadline = connection::WebsocketIdleDeadline::new(&entries, config.idle_timeout())
            .change_context(WebsocketsConnectionIngestorPlanError::Configuration)?;
        Ok(Self {
            endpoint: WebsocketEndpoint {
                raw: parsed.to_string(),
                host: host.to_string(),
                port,
                requires_tls: endpoint_requires_tls,
            },
            dns,
            entries,
            signaling_protocol,
            headers,
            connect_timeout,
            ping_interval: config.ping_interval(),
            idle_deadline,
            handshake_capacity: config.handshake_buffer_capacity(),
            transport_config: WebSocketConfig {
                max_message_size: Some(config.max_message_size()),
                max_frame_size: Some(config.max_frame_size()),
                ..WebSocketConfig::default()
            },
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint.raw
    }
}

pub struct WebsocketsConnectionIngestorMessage {
    frame: WebsocketDataFrame,
    position: (),
    // Messages and quiesce buffers may outlive the connection that supplied its upgrade headers.
    headers: Arc<RetainedIngestHeaders>,
}

impl WebsocketsConnectionIngestorMessage {
    fn new(frame: WebsocketDataFrame, headers: Arc<RetainedIngestHeaders>) -> Self {
        Self {
            frame,
            position: (),
            headers,
        }
    }
}

impl IngestorMessage for WebsocketsConnectionIngestorMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        self.frame.payload()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        self.headers.as_ref()
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow {
            fields: self,
            headers: IngestRowHeaders::Shared(&self.headers),
            position: None,
        }
    }
}

/// The value of the metadata `WEBSOCKETS CONNECTION` declares: the text or binary kind of the data
/// frame.
impl IngestMessageMetadata for WebsocketsConnectionIngestorMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        values.string(self.frame.frame_type().as_ref());
    }
}

pub struct WebsocketsConnectionIngestor {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    connect_timeout: ConfiguredDeadline,
    tls_connector: Option<Connector>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    connection: Option<WebsocketConnection>,
    pending: handshake::HandshakeFrames,
    headers: WebsocketsUpgradeHeaders,
    ping_interval: Option<Duration>,
    idle_deadline: connection::WebsocketIdleDeadline,
    transport_config: WebSocketConfig,
}

mod connection;
mod handshake;
use connection::{WebsocketConnection, WebsocketRead};

enum WebsocketPingOutcome {
    Sent,
    Interrupted,
}

impl WebsocketsConnectionIngestor {
    async fn send_ping(
        &mut self,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<WebsocketPingOutcome> {
        let connection = self
            .connection
            .as_mut()
            .verified("the installed connection requested its ping above");
        let write = timeout(
            WEBSOCKETS_CONTROL_FRAME_BUDGET,
            connection.relay.send(Message::Ping(Vec::new())),
        );
        let Some(outcome) = cancellation.unless_cancelled(write).await else {
            return Ok(WebsocketPingOutcome::Interrupted);
        };
        match outcome {
            Ok(Ok(())) => Ok(WebsocketPingOutcome::Sent),
            Ok(Err(error)) => {
                self.connection = None;
                Err(Report::new(error).change_context(IngestorError::Read {
                    connector: WEBSOCKETS.keyword(),
                }))
            }
            Err(error) => {
                self.connection = None;
                Err(Report::new(error)
                    .change_context(WebsocketsConnectionIngestorFailure::PingTimeout {
                        budget: WEBSOCKETS_CONTROL_FRAME_BUDGET,
                    })
                    .change_context(IngestorError::Read {
                        connector: WEBSOCKETS.keyword(),
                    }))
            }
        }
    }

    /// Why a resume opened no connection: when the attempts ran out of the client's
    /// `connect_timeout_ms`, the failure names it beneath the resume's.
    fn unconnected(
        &self,
        budget: &OperationBudget,
        failures: Vec<String>,
    ) -> Report<IngestorError> {
        let resume = IngestorError::Resume {
            connector: WEBSOCKETS.keyword(),
        };
        let mut report = if budget.remaining().is_zero() {
            Report::new(
                self.connect_timeout
                    .elapsed("opening the WebSocket connection"),
            )
            .change_context(resume)
        } else {
            Report::new(resume)
        };
        for failure in failures {
            report = report.attach_printable(failure);
        }
        report
    }

    async fn close_transport(
        &mut self,
    ) -> error_stack::Result<(), WebsocketsConnectionIngestorFailure> {
        // Detach before the first await: cancelling the handshake drops this exact socket and
        // leaves the source needing a resume, without attaching to another transport.
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        let relay = &mut connection.relay;
        let closing = async {
            relay
                .close(None)
                .await
                .change_context(WebsocketsConnectionIngestorFailure::CloseTransport)?;
            loop {
                nervix_primitives::task::consume_budget().await;
                match relay.next().await {
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(_)) => {}
                    Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)) => {
                        return Ok(());
                    }
                    Some(Err(error)) => {
                        return Err(Report::new(error)
                            .change_context(WebsocketsConnectionIngestorFailure::CloseTransport));
                    }
                }
            }
        };
        timeout(WEBSOCKETS_CONTROL_FRAME_BUDGET, closing)
            .await
            .map_err(|error| {
                Report::new(error).change_context(
                    WebsocketsConnectionIngestorFailure::CloseTimeout {
                        budget: WEBSOCKETS_CONTROL_FRAME_BUDGET,
                    },
                )
            })?
    }
}

#[async_trait]
impl IngestorConnector for WebsocketsConnectionIngestor {
    const KIND: nervix_models::IngestorEntityKind = nervix_models::IngestorEntityKind::Websockets(
        nervix_models::WebsocketsIngestorEntity::Connection,
    );

    type Plan = WebsocketsConnectionIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        let tls_connector = if plan.endpoint.requires_tls {
            let config = RustlsClientConfigProvider::new(&plan.entries)
                .build_with_default_roots()
                .change_context(IngestorError::Open {
                    connector: WEBSOCKETS.keyword(),
                })?;
            Some(Connector::Rustls(config))
        } else {
            None
        };
        Ok(Self {
            endpoint: plan.endpoint.clone(),
            dns: plan.dns.clone(),
            connect_timeout: plan.connect_timeout,
            tls_connector,
            signaling_protocol: plan.signaling_protocol.clone(),
            connection: None,
            pending: handshake::HandshakeFrames::new(plan.handshake_capacity),
            headers: plan.headers.clone(),
            ping_interval: plan.ping_interval,
            idle_deadline: plan.idle_deadline,
            transport_config: plan.transport_config,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.connection.is_none() && self.pending.is_empty()
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.close_transport()
            .await
            .change_context(IngestorError::Suspend {
                connector: WEBSOCKETS.keyword(),
            })
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.connection.is_some() || !self.pending.is_empty() {
            return Ok(IngestorResume::Ready);
        }
        let budget = OperationBudget::start(self.connect_timeout.bound(), LookupShare::Whole);
        let mut request = self
            .endpoint
            .raw
            .as_str()
            .into_client_request()
            .change_context(IngestorError::Resume {
                connector: WEBSOCKETS.keyword(),
            })?;
        WebsocketConnection::install_request_headers(&mut request, &self.headers);
        let addresses = self
            .dns
            .resolve(&self.endpoint.host, self.endpoint.port, budget.lookup())
            .await
            .change_context(IngestorError::Resume {
                connector: WEBSOCKETS.keyword(),
            })?;
        let mut failures = Vec::new();
        let mut connected = None;
        for attempt in budget.attempts(&addresses) {
            nervix_primitives::task::consume_budget().await;
            let result = timeout(attempt.budget, async {
                let stream = TcpStream::connect(attempt.address)
                    .await
                    .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
                client_async_tls_with_config(
                    request.clone(),
                    stream,
                    Some(self.transport_config),
                    self.tls_connector.clone(),
                )
                .await
            })
            .await;
            match result {
                Ok(Ok(connection)) => {
                    connected = Some(connection);
                    break;
                }
                Ok(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                    ProtocolError::SecWebSocketSubProtocolError(refusal),
                ))) => {
                    // The endpoint answered the upgrade and refused its offer. Another address of
                    // the same endpoint answers the same offer, so no further address is tried.
                    return Err(
                        Report::new(WebsocketsConnectionIngestorFailure::from(refusal))
                            .attach(refusal)
                            .attach_printable(attempt.address)
                            .change_context(IngestorError::Resume {
                                connector: WEBSOCKETS.keyword(),
                            }),
                    );
                }
                Ok(Err(error)) => failures.push(format!("{}: {error}", attempt.address)),
                Err(_) => failures.push(format!(
                    "{}: no WebSocket connection within {:?}",
                    attempt.address, attempt.budget
                )),
            }
        }
        let Some((mut relay, response)) = connected else {
            return Err(self.unconnected(&budget, failures));
        };
        let headers = WebsocketConnection::capture_headers(response.headers());
        if let Some(protocol) = self.signaling_protocol.as_ref() {
            let mut pending = self.pending.for_handshake();
            let sink = pending.sink(headers.clone());
            let session = WebsocketSignalingSession::new(protocol.clone());
            self.pending = pending
                .complete(session.run(&mut relay, &sink))
                .await
                .change_context(IngestorError::Resume {
                    connector: WEBSOCKETS.keyword(),
                })?;
        }
        self.connection = Some(WebsocketConnection::new(
            relay,
            headers,
            self.ping_interval,
            self.idle_deadline,
        ));
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.pending.clear();
        self.close_transport()
            .await
            .change_context(IngestorError::Close {
                connector: WEBSOCKETS.keyword(),
            })
    }
}

#[async_trait]
impl BrokerIngestorConnector for WebsocketsConnectionIngestor {
    type Message = WebsocketsConnectionIngestorMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        if let Some(message) = self.pending.pop_front() {
            return Ok(IngestorBatch::Messages(vec![message]));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(connection) = self.connection.as_mut() else {
                return Ok(IngestorBatch::ResumeRequired);
            };
            let Some(read) = cancellation.unless_cancelled(connection.next()).await else {
                return Ok(IngestorBatch::Interrupted {
                    gathered: Vec::new(),
                });
            };
            let read = match read {
                Ok(read) => read,
                Err(report) => {
                    self.connection = None;
                    return Err(report);
                }
            };
            match read {
                WebsocketRead::Ping => match self.send_ping(cancellation).await? {
                    WebsocketPingOutcome::Sent => {}
                    WebsocketPingOutcome::Interrupted => {
                        return Ok(IngestorBatch::Interrupted {
                            gathered: Vec::new(),
                        });
                    }
                },
                WebsocketRead::Frame(Some(Message::Text(text))) => {
                    let headers = self
                        .connection
                        .as_ref()
                        .verified("the installed connection delivered this frame")
                        .headers
                        .clone();
                    return Ok(IngestorBatch::Messages(vec![
                        WebsocketsConnectionIngestorMessage::new(
                            WebsocketDataFrame::Text(text),
                            headers,
                        ),
                    ]));
                }
                WebsocketRead::Frame(Some(Message::Binary(bytes))) => {
                    let headers = self
                        .connection
                        .as_ref()
                        .verified("the installed connection delivered this frame")
                        .headers
                        .clone();
                    return Ok(IngestorBatch::Messages(vec![
                        WebsocketsConnectionIngestorMessage::new(
                            WebsocketDataFrame::Binary(bytes),
                            headers,
                        ),
                    ]));
                }
                WebsocketRead::Frame(Some(Message::Close(frame))) => {
                    let failure = match frame {
                        Some(frame) => WebsocketsConnectionIngestorFailure::PeerClosed {
                            code: u16::from(frame.code),
                            reason: frame.reason.into_owned(),
                        },
                        None => WebsocketsConnectionIngestorFailure::PeerClosedWithoutStatus,
                    };
                    let mut connection = self
                        .connection
                        .take()
                        .verified("the installed transport yielded the Close above");
                    let answered =
                        timeout(WEBSOCKETS_CONTROL_FRAME_BUDGET, connection.relay.flush()).await;
                    let report = Report::new(failure);
                    let report = match answered {
                        Ok(Ok(())) => report,
                        Ok(Err(error)) => report.attach_printable(error),
                        Err(error) => report.attach_printable(error),
                    };
                    return Err(report.change_context(IngestorError::Read {
                        connector: WEBSOCKETS.keyword(),
                    }));
                }
                WebsocketRead::Frame(None) => {
                    self.connection = None;
                    return Err(Report::new(WebsocketsConnectionIngestorFailure::Ended)
                        .change_context(IngestorError::Read {
                            connector: WEBSOCKETS.keyword(),
                        }));
                }
                WebsocketRead::Frame(Some(
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_),
                )) => {}
            }
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{net::IpAddr, time::Duration};

    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_primitives::net::TcpListener;
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_tungstenite::{WebSocketStream, tungstenite::handshake::derive_accept_key};

    use super::*;

    #[nervix_primitives::test]
    async fn a_server_that_never_answers_the_upgrade_is_reported_within_connect_timeout() {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        // Accepts the connection and never answers the upgrade request.
        let silent = nervix_primitives::task::spawn(async move {
            let (held, _) = listener.accept().await.assured("the source connects");
            std::future::pending::<()>().await;
            drop(held);
        });
        let mut entries = endpoint(format!("ws://{address}/socket"));
        entries.push(ClientConfigEntry {
            key: "connect_timeout_ms".to_string(),
            value: "200".to_string(),
        });
        let plan = WebsocketsConnectionIngestorPlan::new(
            entries,
            WebsocketsUpgradeHeaders::default(),
            None,
            fixture.resolver,
        )
        .assured("the endpoint and deadline are valid");
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source opens");
        let failure = timeout(Duration::from_secs(10), source.resume())
            .await
            .assured("connect_timeout_ms bounds the resume")
            .err()
            .assured("a server that never answers cannot be connected");
        let elapsed = failure
            .frames()
            .find_map(|frame| frame.downcast_ref::<nervix_connector::DeadlineElapsed>())
            .assured("the elapsed deadline is the innermost cause");
        assert_eq!(elapsed.key, nervix_models::DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));
        assert!(format!("{failure:#}").contains("within connect_timeout_ms 200ms"));
        silent.abort();
    }

    #[nervix_primitives::test]
    async fn an_invalid_connect_timeout_refuses_the_plan() {
        let fixture = DnsFixture::start().await;
        let mut entries = endpoint("ws://127.0.0.1:9/socket".to_string());
        entries.push(ClientConfigEntry {
            key: "connect_timeout_ms".to_string(),
            value: "0".to_string(),
        });
        let error = WebsocketsConnectionIngestorPlan::new(
            entries,
            WebsocketsUpgradeHeaders::default(),
            None,
            fixture.resolver,
        )
        .err()
        .assured("a zero deadline cannot plan a connection");
        assert!(matches!(
            error.current_context(),
            WebsocketsConnectionIngestorPlanError::Configuration
        ));
    }

    async fn connected_pair() -> (WebsocketsConnectionIngestor, WebSocketStream<TcpStream>) {
        connected_pair_with(Vec::new(), WebsocketsUpgradeHeaders::default()).await
    }

    pub(super) async fn connected_pair_with(
        mut config: Vec<ClientConfigEntry>,
        headers: WebsocketsUpgradeHeaders,
    ) -> (WebsocketsConnectionIngestor, WebSocketStream<TcpStream>) {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        config.extend(endpoint(format!("ws://{address}/socket")));
        let expected: Vec<_> = headers
            .iter()
            .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
            .collect();
        let plan = WebsocketsConnectionIngestorPlan::new(config, headers, None, fixture.resolver)
            .assured("the endpoint is valid");
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source opens");
        let server = async {
            let (stream, _) = listener.accept().await.assured("the source connects");
            #[expect(clippy::result_large_err, reason = "tungstenite requires an unboxed HTTP rejection response in its handshake callback")]
            let verify_upgrade = |request: &tokio_tungstenite::tungstenite::handshake::server::Request, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                for (name, value) in &expected {
                    assert_eq!(request.headers().get(name).assured("the request contains every declared application field").as_bytes(), value.as_bytes());
                }
                response.headers_mut().insert("x-nervix-upgrade", "fixture".parse().assured("fixture is a valid header value"));
                Ok(response)
            };
            tokio_tungstenite::accept_hdr_async(stream, verify_upgrade)
                .await
                .assured("the upgrade succeeds")
        };
        let (resumed, peer) = tokio::join!(source.resume(), server);
        assert_eq!(resumed.assured("the source resumes"), IngestorResume::Ready);
        (source, peer)
    }

    #[nervix_primitives::test]
    async fn suspension_and_close_exchange_close_frames() {
        use futures_util::{SinkExt as _, StreamExt as _};

        for suspend in [true, false] {
            let (mut source, mut peer) = connected_pair().await;
            let headers = source
                .connection
                .as_ref()
                .assured("the pair is connected")
                .headers
                .clone();
            source
                .pending
                .push_back(WebsocketsConnectionIngestorMessage::new(
                    WebsocketDataFrame::Binary(b"retained".to_vec()),
                    headers,
                ));
            let ending = async {
                if suspend {
                    source.suspend().await
                } else {
                    source.close().await
                }
            };
            let observe = async {
                let frame = peer
                    .next()
                    .await
                    .assured("the peer receives a frame")
                    .assured("the frame is valid");
                assert!(
                    matches!(frame, Message::Close(_)),
                    "expected Close, got {frame:?}"
                );
                match peer.flush().await {
                    Ok(()) | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => {}
                    Err(error) => panic!("the peer could not answer the Close: {error}"),
                }
            };
            let (closed, ()) = tokio::join!(ending, observe);
            closed.assured("the close handshake succeeds");
            assert_eq!(source.pending.len(), usize::from(suspend));
            if suspend {
                let batch = source
                    .next_batch(
                        IngestorBatchRequest {
                            max_messages: std::num::NonZeroUsize::MIN,
                            batch_timeout: None,
                        },
                        &IngestorReadCancellation::new(),
                    )
                    .await
                    .assured("the suspended source retains its accepted frame");
                let IngestorBatch::Messages(messages) = batch else {
                    panic!("the retained frame is ready before another connection");
                };
                assert_eq!(messages[0].payload(), b"retained");
            }
            assert!(source.needs_resume());
        }
    }

    #[nervix_primitives::test]
    async fn a_cancelled_read_takes_no_frame_and_leaves_the_next_one_to_the_next_read() {
        use futures_util::SinkExt as _;

        let (mut source, mut peer) = connected_pair().await;
        let request = IngestorBatchRequest {
            max_messages: std::num::NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let cancellation = IngestorReadCancellation::new();
        cancellation.cancel();
        let interrupted = source
            .next_batch(request, &cancellation)
            .await
            .assured("a cancelled read is not a failure");
        let IngestorBatch::Interrupted { gathered } = interrupted else {
            panic!("a read the host cancelled ends as interrupted");
        };
        assert!(gathered.is_empty(), "the peer sent nothing to take");

        peer.send(Message::Text("sent after the cancelled read".into()))
            .await
            .assured("the peer sends a frame");
        let batch = source
            .next_batch(request, &IngestorReadCancellation::new())
            .await
            .assured("the frame is read");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("the source reads the frame as a batch");
        };
        let message = messages.first().assured("the read returned the frame");
        assert_eq!(message.payload(), b"sent after the cancelled read");
        assert_eq!(messages.len(), 1);
    }

    #[nervix_primitives::test]
    async fn a_peer_close_is_a_transient_read_failure_with_its_status() {
        use futures_util::StreamExt as _;
        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

        let (mut source, mut peer) = connected_pair().await;
        peer.close(Some(CloseFrame {
            code: CloseCode::Again,
            reason: "fixture restart".into(),
        }))
        .await
        .assured("the peer sends its close");
        let error = source
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await
            .err()
            .assured("a peer close must reach the host's transient failure path");
        assert!(matches!(
            error.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(format!("{error:#}").contains("1013"));
        assert!(format!("{error:#}").contains("fixture restart"));
        assert!(source.needs_resume());
        let answer = peer
            .next()
            .await
            .assured("the client answers the peer's Close")
            .assured("the closing frame is valid");
        let Message::Close(Some(frame)) = answer else {
            panic!("expected the client's closing answer, got {answer:?}");
        };
        assert_eq!(u16::from(frame.code), 1013);
        assert_eq!(frame.reason, "fixture restart");
    }

    #[nervix_primitives::test]
    async fn a_close_without_a_status_is_a_transient_read_failure() {
        let (mut source, mut peer) = connected_pair().await;
        peer.close(None)
            .await
            .assured("the peer sends an empty Close");
        let error = source
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await
            .err()
            .assured("an empty Close still requires a paced reconnect");
        assert!(format!("{error:#}").contains("without a status"));
        assert!(source.needs_resume());
    }

    #[nervix_primitives::test]
    async fn a_peer_that_never_answers_close_releases_the_transport_with_a_timeout() {
        for suspend in [true, false] {
            let (mut source, _peer) = connected_pair().await;
            let headers = source
                .connection
                .as_ref()
                .assured("the pair is connected")
                .headers
                .clone();
            source
                .pending
                .push_back(WebsocketsConnectionIngestorMessage::new(
                    WebsocketDataFrame::Binary(b"retained".to_vec()),
                    headers,
                ));
            let result = if suspend {
                timeout(Duration::from_secs(60), source.suspend()).await
            } else {
                timeout(Duration::from_secs(60), source.close()).await
            };
            let error = result
                .assured("source teardown has its own bounded close wait")
                .err()
                .assured("a peer that never reads cannot answer the Close");
            assert!(format!("{error:#}").contains("close handshake"));
            assert_eq!(source.pending.len(), usize::from(suspend));
            if suspend {
                let batch = source
                    .next_batch(
                        IngestorBatchRequest {
                            max_messages: std::num::NonZeroUsize::MIN,
                            batch_timeout: None,
                        },
                        &IngestorReadCancellation::new(),
                    )
                    .await
                    .assured("a failed closing wait keeps the suspended source's accepted frame");
                let IngestorBatch::Messages(messages) = batch else {
                    panic!("the retained frame is ready before another connection");
                };
                assert_eq!(messages[0].payload(), b"retained");
            }
            assert!(source.needs_resume());
        }
    }

    #[nervix_primitives::test]
    async fn a_cancelled_close_releases_the_transport_and_clears_pending_payloads() {
        let (mut source, _peer) = connected_pair().await;
        let headers = source
            .connection
            .as_ref()
            .assured("the pair is connected")
            .headers
            .clone();
        source
            .pending
            .push_back(WebsocketsConnectionIngestorMessage::new(
                WebsocketDataFrame::Binary(b"retained".to_vec()),
                headers,
            ));
        assert!(
            timeout(Duration::from_millis(1), source.close())
                .await
                .is_err()
        );
        assert!(source.needs_resume());
        assert!(source.pending.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_failed_read_preserves_its_typed_transport_cause() {
        let (mut source, peer) = connected_pair().await;
        drop(peer);
        let error = source
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await
            .err()
            .assured("TCP ended without the closing handshake");
        assert!(
            error
                .downcast_ref::<tokio_tungstenite::tungstenite::Error>()
                .is_some()
        );
        assert!(source.needs_resume());
    }

    struct DnsFixture {
        authority: DnsAuthority,
        resolver: DnsResolver,
        _files: TempDir,
    }

    impl DnsFixture {
        async fn start() -> Self {
            let authority = DnsAuthority::start_on_loopback()
                .await
                .assured("a loopback UDP port is available");
            let files = tempfile::tempdir().assured("a temporary directory can be created");
            let resolver_configuration = files.path().join("resolv.conf");
            let hosts_file = files.path().join("hosts");
            std::fs::write(
                &resolver_configuration,
                "search --\noptions ndots:1 timeout:1 attempts:1\n",
            )
            .assured("the fixture resolver configuration can be written");
            std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
            let resolver = DnsResolver::load(DnsConfiguration {
                resolver_configuration,
                hosts_file,
                name_servers: NameServers::Explicit(vec![authority.address()]),
            })
            .await
            .assured("the fixture resolver configuration is valid");
            Self {
                authority,
                resolver,
                _files: files,
            }
        }

        fn answer(&self, addresses: Vec<IpAddr>) {
            self.authority.set(
                "websocket.nervix.test",
                DnsAnswer::Addresses {
                    addresses,
                    ttl: Duration::from_secs(1),
                },
            );
        }
    }

    fn endpoint(value: String) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value,
        }]
    }

    #[nervix_primitives::test]
    async fn resume_tries_the_next_address_and_preserves_host_path_and_query() {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("127.0.8.2:0")
            .await
            .assured("a loopback TCP port is available");
        let port = listener
            .local_addr()
            .assured("the listener is bound")
            .port();
        fixture.answer(vec![
            "127.0.8.1".parse().assured("literal IPv4 address"),
            "127.0.8.2".parse().assured("literal IPv4 address"),
        ]);
        let endpoint_url = format!("ws://websocket.nervix.test:{port}/socket/path?item=42");
        let plan = WebsocketsConnectionIngestorPlan::new(
            endpoint(endpoint_url),
            WebsocketsUpgradeHeaders::default(),
            None,
            fixture.resolver.clone(),
        )
        .assured("the WebSocket endpoint is valid");
        let server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .assured("the second DNS answer was dialled");
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut chunk = [0_u8; 4096];
                let count = stream
                    .read(&mut chunk)
                    .await
                    .assured("the client sends an upgrade request");
                assert!(count > 0 && bytes.len() < 8192);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let request = std::str::from_utf8(&bytes).assured("HTTP headers are ASCII");
            let path = request
                .lines()
                .next()
                .assured("the request has a start line")
                .split_whitespace()
                .nth(1)
                .assured("the start line includes a path")
                .to_string();
            let header = |name: &str| {
                request
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(key, value)| {
                            key.eq_ignore_ascii_case(name).then_some(value.trim())
                        })
                    })
                    .assured("the upgrade request includes the required header")
            };
            let authority = header("Host").to_string();
            let accept = derive_accept_key(header("Sec-WebSocket-Key").as_bytes());
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: \
                         Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .assured("the upgrade response is sent");
            (authority, path)
        });
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source can open");
        assert_eq!(
            source
                .resume()
                .await
                .assured("the second address is reachable"),
            IngestorResume::Ready
        );
        let (authority, path) = server.await.assured("the server task completes");
        assert_eq!(authority, format!("websocket.nervix.test:{port}"));
        assert_eq!(path, "/socket/path?item=42");
        assert!(fixture.authority.questions_for("websocket.nervix.test") > 0);
    }

    /// Accepts one upgrade and answers its subprotocol offer with `answer`, reporting the offer
    /// it read and the server side of the upgrade, which a refusing client ends at once.
    async fn answer_subprotocol(
        listener: &TcpListener,
        answer: Option<&'static str>,
    ) -> (
        Option<String>,
        Result<WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error>,
    ) {
        let (stream, _) = listener.accept().await.assured("the source connects");
        let mut offer = None;
        #[expect(
            clippy::result_large_err,
            reason = "tungstenite requires an unboxed HTTP rejection response in its handshake \
                      callback"
        )]
        let answer_upgrade = |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                              mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if let Some(value) = request.headers().get("sec-websocket-protocol") {
                let value = value.to_str().assured("the validated offer is visible ASCII");
                offer = Some(value.to_string());
            }
            if let Some(protocol) = answer {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    protocol.parse().assured("the scripted answer is a header value"),
                );
            }
            Ok(response)
        };
        let upgraded = tokio_tungstenite::accept_hdr_async(stream, answer_upgrade).await;
        (offer, upgraded)
    }

    fn offering(offer: &str) -> WebsocketsUpgradeHeaders {
        WebsocketsUpgradeHeaders::parse(&[nervix_models::WebsocketsUpgradeHeader {
            name: "Sec-WebSocket-Protocol".to_string(),
            value: offer.to_string(),
        }])
        .assured("the offer lists unique subprotocol tokens")
    }

    #[nervix_primitives::test]
    async fn the_selected_subprotocol_reaches_every_message_header_snapshot() {
        use futures_util::SinkExt as _;

        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        let plan = WebsocketsConnectionIngestorPlan::new(
            endpoint(format!("ws://{address}/socket")),
            offering("v2.events, v1.events"),
            None,
            fixture.resolver,
        )
        .assured("the endpoint is valid");
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source opens");
        let (resumed, (offer, upgraded)) = tokio::join!(
            source.resume(),
            answer_subprotocol(&listener, Some("v1.events"))
        );
        assert_eq!(
            resumed.assured("the peer selected an offered subprotocol"),
            IngestorResume::Ready
        );
        // The offer is written as its tokens joined by commas alone.
        assert_eq!(offer.as_deref(), Some("v2.events,v1.events"));
        let mut peer = upgraded.assured("the upgrade succeeds");
        peer.send(Message::Text("payload".to_string()))
            .await
            .assured("the peer sends a data frame");
        let batch = source
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await
            .assured("the data frame is read");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("the peer delivered a data frame");
        };
        let message = messages.first().assured("the read returned the frame");
        let mut selected = Vec::new();
        message.headers().visit(&mut |name, value| {
            if name == "sec-websocket-protocol" {
                selected.push(value.to_string());
            }
        });
        assert_eq!(selected, ["v1.events"]);
    }

    #[nervix_primitives::test]
    async fn an_answer_that_contradicts_the_offer_fails_resume_with_its_typed_refusal() {
        struct Case {
            offer: Option<&'static str>,
            answer: Option<&'static str>,
            refusal: SubProtocolError,
        }
        for case in [
            Case {
                offer: Some("v2.events, v1.events"),
                answer: Some("v3.events"),
                refusal: SubProtocolError::InvalidSubProtocol,
            },
            Case {
                offer: Some("v2.events, v1.events"),
                answer: None,
                refusal: SubProtocolError::NoSubProtocol,
            },
            Case {
                offer: None,
                answer: Some("v1.events"),
                refusal: SubProtocolError::ServerSentSubProtocolNoneRequested,
            },
        ] {
            let fixture = DnsFixture::start().await;
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .assured("a loopback TCP port is available");
            let address = listener.local_addr().assured("the listener is bound");
            let headers = match case.offer {
                Some(offer) => offering(offer),
                None => WebsocketsUpgradeHeaders::default(),
            };
            let plan = WebsocketsConnectionIngestorPlan::new(
                endpoint(format!("ws://{address}/socket")),
                headers,
                None,
                fixture.resolver,
            )
            .assured("the endpoint is valid");
            let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
                .await
                .assured("the source opens");
            let (resumed, _) =
                tokio::join!(source.resume(), answer_subprotocol(&listener, case.answer));
            let failure = resumed
                .err()
                .assured("an answer that contradicts the request fails the connection");
            assert!(matches!(
                failure.current_context(),
                IngestorError::Resume { .. }
            ));
            let reported = failure
                .frames()
                .find_map(|frame| frame.downcast_ref::<WebsocketsConnectionIngestorFailure>())
                .assured("the typed refusal is the resume failure's cause");
            let expected = WebsocketsConnectionIngestorFailure::from(case.refusal);
            assert_eq!(
                std::mem::discriminant(reported),
                std::mem::discriminant(&expected)
            );
            assert_eq!(
                failure
                    .frames()
                    .find_map(|frame| frame.downcast_ref::<SubProtocolError>()),
                Some(&case.refusal),
                "the transport's refusal stays attached for typed inspection"
            );
            // An offer can carry a credential, so the refusal never prints it.
            assert!(!format!("{failure:#}").contains("events"));
            assert!(
                source.needs_resume(),
                "a refused upgrade installs no connection"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_refused_offer_tries_no_further_address() {
        let fixture = DnsFixture::start().await;
        let refusing = TcpListener::bind("127.0.8.3:0")
            .await
            .assured("a loopback TCP port is available");
        let port = refusing
            .local_addr()
            .assured("the listener is bound")
            .port();
        // The second address would select an offered subprotocol, so dialling it would connect.
        let accepting = TcpListener::bind(std::net::SocketAddr::from(([127, 0, 8, 4], port)))
            .await
            .assured("the same port is available on a second loopback address");
        fixture.answer(vec![
            "127.0.8.3".parse().assured("literal IPv4 address"),
            "127.0.8.4".parse().assured("literal IPv4 address"),
        ]);
        let plan = WebsocketsConnectionIngestorPlan::new(
            endpoint(format!("ws://websocket.nervix.test:{port}/socket")),
            offering("v1.events"),
            None,
            fixture.resolver.clone(),
        )
        .assured("the WebSocket endpoint is valid");
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source opens");
        let (resumed, _) = tokio::join!(
            source.resume(),
            answer_subprotocol(&refusing, Some("v2.events"))
        );
        let failure = resumed.err().assured("the first address refused the offer");
        assert!(matches!(
            failure
                .frames()
                .find_map(|frame| frame.downcast_ref::<WebsocketsConnectionIngestorFailure>()),
            Some(WebsocketsConnectionIngestorFailure::SubprotocolNotOffered)
        ));
        let dialled = timeout(Duration::from_millis(200), accepting.accept()).await;
        assert!(
            dialled.is_err(),
            "the endpoint refused the offer, so the second address is never dialled"
        );
    }

    #[nervix_primitives::test]
    async fn resume_connects_to_a_literal_ipv6_endpoint_without_a_dns_question() {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("[::1]:0")
            .await
            .assured("the IPv6 loopback address is available");
        let address = listener.local_addr().assured("the listener is bound");
        let plan = WebsocketsConnectionIngestorPlan::new(
            endpoint(format!("ws://{address}/socket")),
            WebsocketsUpgradeHeaders::default(),
            None,
            fixture.resolver.clone(),
        )
        .assured("the literal IPv6 endpoint is valid");
        let server = nervix_primitives::task::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .assured("the literal address was dialled");
            let socket = tokio_tungstenite::accept_async(stream)
                .await
                .assured("the WebSocket upgrade succeeds");
            drop(socket);
        });
        let mut source = WebsocketsConnectionIngestor::open(&plan, 0)
            .await
            .assured("the source can open");
        assert_eq!(
            source
                .resume()
                .await
                .assured("the IPv6 endpoint is reachable"),
            IngestorResume::Ready
        );
        server.await.assured("the server task completes");
        assert_eq!(fixture.authority.total_questions(), 0);
    }

    #[nervix_primitives::test]
    async fn source_plan_uses_url_default_ports_and_rejects_other_schemes() {
        let fixture = DnsFixture::start().await;
        for (url, port, requires_tls) in [
            ("ws://websocket.nervix.test/path", 80, false),
            ("wss://websocket.nervix.test/path", 443, true),
        ] {
            let plan = WebsocketsConnectionIngestorPlan::new(
                endpoint(url.to_string()),
                WebsocketsUpgradeHeaders::default(),
                None,
                fixture.resolver.clone(),
            )
            .assured("the WebSocket URL is valid");
            assert_eq!(plan.endpoint.port, port);
            assert_eq!(plan.endpoint.requires_tls, requires_tls);
            assert_eq!(plan.endpoint.raw, url);
        }
        let error = WebsocketsConnectionIngestorPlan::new(
            endpoint("http://websocket.nervix.test/path".to_string()),
            WebsocketsUpgradeHeaders::default(),
            None,
            fixture.resolver.clone(),
        )
        .err()
        .assured("HTTP is not a WebSocket endpoint");
        assert!(matches!(
            error.current_context(),
            WebsocketsConnectionIngestorPlanError::Configuration
        ));
    }
}

#[cfg(test)]
mod connection_tests;
