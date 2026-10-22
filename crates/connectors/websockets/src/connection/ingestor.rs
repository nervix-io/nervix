//! WebSocket client source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Outbound transport requirements and configuration, DNS-backed resume within the
//!   client's `connect_timeout_ms`, signaling before readiness, accepted-frame storage, payload
//!   delivery, and suspend and close operations.
//! - **Depends on.** The source contract, typed plans and host services, compiled signaling engine,
//!   vocabulary values, execution primitives, TLS configuration, and WebSocket transport.
//! - **Must not know.** Configuration Models, NSPL, runtime collectors, relays, branches,
//!   schedules, registry state, endpoint listeners, or host intake, admission and retry loops.
//!
//! Current boundary violation: the composer hard-codes the source's buffering, flush and
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

use std::{collections::VecDeque, future::Future, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, ClientConfigResult, ConfiguredDeadline, IngestMessageHeaders,
    IngestMetadataRow, IngestorBatch, IngestorBatchRequest, IngestorConnector, IngestorError,
    IngestorMessage, IngestorReadCancellation, IngestorResult, IngestorResume, NoIngestHeaders,
    RustlsClientConfigProvider, client_config_value,
};
use nervix_dns::{DnsResolver, LookupShare, OperationBudget};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{
    net::TcpStream,
    sync::{Arc, mpsc},
    time::timeout,
};
use thiserror::Error;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config, tungstenite::Message,
};
use url::Url;

use crate::connection::{
    CompiledSignalingProtocol, SignalingDataEmitter, WebsocketSignalingSession,
};

const WEBSOCKETS: &str = "websockets";
const CLOSE_BUDGET: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
enum WebsocketIngestorFailure {
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
}

#[derive(Debug, Error)]
pub enum WebsocketIngestorPlanError {
    #[error("invalid WebSockets client configuration")]
    Configuration,
    #[error("invalid WebSockets endpoint")]
    Endpoint,
    #[error("unsupported WebSockets endpoint scheme '{scheme}', expected ws:// or wss://")]
    Scheme { scheme: String },
}

#[derive(Clone)]
pub struct WebsocketIngestorPlan {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    entries: Vec<ClientConfigEntry>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
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

impl WebsocketIngestorPlan {
    pub fn new(
        entries: Vec<ClientConfigEntry>,
        signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
        dns: DnsResolver,
    ) -> Result<Self, Report<WebsocketIngestorPlanError>> {
        let endpoint = Self::endpoint_from_config(&entries)
            .change_context(WebsocketIngestorPlanError::Configuration)?;
        let connect_timeout = ConfiguredDeadline::connect(&entries)
            .change_context(WebsocketIngestorPlanError::Configuration)?;
        let parsed = Url::parse(&endpoint).map_err(|error| {
            Report::new(WebsocketIngestorPlanError::Endpoint).attach_printable(error)
        })?;
        let endpoint_requires_tls = match parsed.scheme() {
            "ws" => false,
            "wss" => true,
            scheme => {
                return Err(Report::new(WebsocketIngestorPlanError::Scheme {
                    scheme: scheme.to_string(),
                }));
            }
        };
        let host = parsed
            .host_str()
            .ok_or_else(|| Report::new(WebsocketIngestorPlanError::Endpoint))?;
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| Report::new(WebsocketIngestorPlanError::Endpoint))?;
        Ok(Self {
            endpoint: WebsocketEndpoint {
                raw: endpoint,
                host: host.to_string(),
                port,
                requires_tls: endpoint_requires_tls,
            },
            dns,
            entries,
            signaling_protocol,
            connect_timeout,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint.raw
    }

    pub fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "WebSockets")
    }
}

pub struct WebsocketIngestorMessage {
    payload: Vec<u8>,
    position: (),
    headers: NoIngestHeaders,
}

impl WebsocketIngestorMessage {
    fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            position: (),
            headers: NoIngestHeaders,
        }
    }
}

impl IngestorMessage for WebsocketIngestorMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

pub struct WebsocketIngestor {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    connect_timeout: ConfiguredDeadline,
    tls_connector: Option<Connector>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    relay: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending: VecDeque<Vec<u8>>,
}

impl WebsocketIngestor {
    /// Why a resume opened no connection: when the attempts ran out of the client's
    /// `connect_timeout_ms`, the failure names it beneath the resume's.
    fn unconnected(
        &self,
        budget: &OperationBudget,
        failures: Vec<String>,
    ) -> Report<IngestorError> {
        let resume = IngestorError::Resume {
            connector: WEBSOCKETS,
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

    async fn close_transport(&mut self) -> error_stack::Result<(), WebsocketIngestorFailure> {
        // Detach before the first await: cancelling the handshake drops this exact socket and
        // leaves the source needing a resume, without attaching to another transport.
        let Some(mut relay) = self.relay.take() else {
            return Ok(());
        };
        let closing = async {
            relay
                .close(None)
                .await
                .change_context(WebsocketIngestorFailure::CloseTransport)?;
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
                            .change_context(WebsocketIngestorFailure::CloseTransport));
                    }
                }
            }
        };
        timeout(CLOSE_BUDGET, closing).await.map_err(|error| {
            Report::new(error).change_context(WebsocketIngestorFailure::CloseTimeout {
                budget: CLOSE_BUDGET,
            })
        })?
    }
}

#[async_trait]
impl IngestorConnector for WebsocketIngestor {
    type Plan = WebsocketIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        let tls_connector = if plan.endpoint.requires_tls {
            let config = RustlsClientConfigProvider::new(&plan.entries)
                .build_with_default_roots()
                .change_context(IngestorError::Open {
                    connector: WEBSOCKETS,
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
            relay: None,
            pending: VecDeque::new(),
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.relay.is_none()
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.close_transport()
            .await
            .change_context(IngestorError::Suspend {
                connector: WEBSOCKETS,
            })
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.relay.is_some() {
            return Ok(IngestorResume::Ready);
        }
        let budget = OperationBudget::start(self.connect_timeout.bound(), LookupShare::Whole);
        let addresses = self
            .dns
            .resolve(&self.endpoint.host, self.endpoint.port, budget.lookup())
            .await
            .change_context(IngestorError::Resume {
                connector: WEBSOCKETS,
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
                    self.endpoint.raw.as_str(),
                    stream,
                    None,
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
                Ok(Err(error)) => failures.push(format!("{}: {error}", attempt.address)),
                Err(_) => failures.push(format!(
                    "{}: no WebSocket connection within {:?}",
                    attempt.address, attempt.budget
                )),
            }
        }
        let Some((mut relay, _)) = connected else {
            return Err(self.unconnected(&budget, failures));
        };
        if let Some(protocol) = self.signaling_protocol.as_ref() {
            let (payloads_tx, mut payloads_rx) = mpsc::unbounded_channel();
            let connector = QueuedSignalingDataEmitter { payloads_tx };
            {
                let mapping_input = WebsocketSignalingSession::new(protocol.clone())
                    .run(&mut relay, &connector)
                    .await;
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(error) => Err({
                        // A frame the peer could not take or send keeps the transport error beneath
                        // the signaling context, so the reason is the whole chain.
                        let reason = format!("{error:#}");
                        error
                            .change_context(IngestorError::Resume {
                                connector: WEBSOCKETS,
                            })
                            .attach_printable(format!("websocket signaling failed: {reason}"))
                    }),
                }
            }?;
            drop(connector);
            while let Ok(payload) = payloads_rx.try_recv() {
                self.pending.push_back(payload);
            }
        }
        self.relay = Some(relay);
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.pending.clear();
        self.close_transport()
            .await
            .change_context(IngestorError::Close {
                connector: WEBSOCKETS,
            })
    }
}

#[async_trait]
impl BrokerIngestorConnector for WebsocketIngestor {
    type Message = WebsocketIngestorMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        if let Some(payload) = self.pending.pop_front() {
            return Ok(IngestorBatch::Messages(vec![
                WebsocketIngestorMessage::new(payload),
            ]));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(relay) = self.relay.as_mut() else {
                return Ok(IngestorBatch::ResumeRequired);
            };
            let next = futures_util::StreamExt::next(relay);
            let Some(message) = cancellation.unless_cancelled(next).await else {
                return Ok(IngestorBatch::Interrupted {
                    gathered: Vec::new(),
                });
            };
            match message {
                Some(Ok(Message::Text(text))) => {
                    return Ok(IngestorBatch::Messages(vec![
                        WebsocketIngestorMessage::new(text.to_string().into_bytes()),
                    ]));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    return Ok(IngestorBatch::Messages(vec![
                        WebsocketIngestorMessage::new(bytes.to_vec()),
                    ]));
                }
                Some(Ok(Message::Close(frame))) => {
                    let failure = match frame {
                        Some(frame) => WebsocketIngestorFailure::PeerClosed {
                            code: u16::from(frame.code),
                            reason: frame.reason.into_owned(),
                        },
                        None => WebsocketIngestorFailure::PeerClosedWithoutStatus,
                    };
                    // Reading a Close queues its response. Flush it before releasing the socket;
                    // any failure to answer remains attached to the peer's original close reason.
                    let mut relay = self
                        .relay
                        .take()
                        .verified("the installed transport yielded the Close above");
                    let answered = timeout(CLOSE_BUDGET, relay.flush()).await;
                    let report = Report::new(failure);
                    let report = match answered {
                        Ok(Ok(())) => report,
                        Ok(Err(error)) => report.attach_printable(error),
                        Err(error) => report.attach_printable(error),
                    };
                    return Err(report.change_context(IngestorError::Read {
                        connector: WEBSOCKETS,
                    }));
                }
                None => {
                    self.relay = None;
                    return Err(Report::new(WebsocketIngestorFailure::Ended).change_context(
                        IngestorError::Read {
                            connector: WEBSOCKETS,
                        },
                    ));
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                Some(Err(error)) => {
                    self.relay = None;
                    return Err(Report::new(error).change_context(IngestorError::Read {
                        connector: WEBSOCKETS,
                    }));
                }
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

struct QueuedSignalingDataEmitter {
    payloads_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl SignalingDataEmitter for QueuedSignalingDataEmitter {
    fn accept(&self, payload: Vec<u8>) -> impl Future<Output = ()> + Send {
        self.payloads_tx
            .send(payload)
            .assured("the signaling session retains its payload receiver while the sink is live");
        std::future::ready(())
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
    use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

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
        let plan = WebsocketIngestorPlan::new(entries, None, fixture.resolver)
            .assured("the endpoint and deadline are valid");
        let mut source = WebsocketIngestor::open(&plan, 0)
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
        let error = WebsocketIngestorPlan::new(entries, None, fixture.resolver)
            .err()
            .assured("a zero deadline cannot plan a connection");
        assert!(matches!(
            error.current_context(),
            WebsocketIngestorPlanError::Configuration
        ));
    }

    async fn connected_pair() -> (WebsocketIngestor, WebSocketStream<TcpStream>) {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        let plan = WebsocketIngestorPlan::new(
            endpoint(format!("ws://{address}/socket")),
            None,
            fixture.resolver,
        )
        .assured("the endpoint is valid");
        let mut source = WebsocketIngestor::open(&plan, 0)
            .await
            .assured("the source opens");
        let server = async {
            let (stream, _) = listener.accept().await.assured("the source connects");
            tokio_tungstenite::accept_async(stream)
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
            source.pending.push_back(b"retained".to_vec());
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
            assert!(source.needs_resume());
            assert_eq!(source.pending.len(), usize::from(suspend));
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
            source.pending.push_back(b"retained".to_vec());
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
            assert!(source.needs_resume());
            assert_eq!(source.pending.len(), usize::from(suspend));
        }
    }

    #[nervix_primitives::test]
    async fn a_cancelled_close_releases_the_transport_and_clears_pending_payloads() {
        let (mut source, _peer) = connected_pair().await;
        source.pending.push_back(b"retained".to_vec());
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
        let plan =
            WebsocketIngestorPlan::new(endpoint(endpoint_url), None, fixture.resolver.clone())
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
        let mut source = WebsocketIngestor::open(&plan, 0)
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

    #[nervix_primitives::test]
    async fn resume_connects_to_a_literal_ipv6_endpoint_without_a_dns_question() {
        let fixture = DnsFixture::start().await;
        let listener = TcpListener::bind("[::1]:0")
            .await
            .assured("the IPv6 loopback address is available");
        let address = listener.local_addr().assured("the listener is bound");
        let plan = WebsocketIngestorPlan::new(
            endpoint(format!("ws://{address}/socket")),
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
        let mut source = WebsocketIngestor::open(&plan, 0)
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
            let plan = WebsocketIngestorPlan::new(
                endpoint(url.to_string()),
                None,
                fixture.resolver.clone(),
            )
            .assured("the WebSocket URL is valid");
            assert_eq!(plan.endpoint.port, port);
            assert_eq!(plan.endpoint.requires_tls, requires_tls);
            assert_eq!(plan.endpoint.raw, url);
        }
        let error = WebsocketIngestorPlan::new(
            endpoint("http://websocket.nervix.test/path".to_string()),
            None,
            fixture.resolver.clone(),
        )
        .err()
        .assured("HTTP is not a WebSocket endpoint");
        assert!(matches!(
            error.current_context(),
            WebsocketIngestorPlanError::Scheme { .. }
        ));
    }

    #[test]
    fn source_plan_reads_its_endpoint_from_the_client_config() {
        let config = vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "wss://example.com/socket".to_string(),
        }];

        assert_eq!(
            WebsocketIngestorPlan::endpoint_from_config(&config).expect("endpoint"),
            "wss://example.com/socket"
        );
    }

    #[test]
    fn source_plan_names_a_missing_endpoint_key() {
        let error = WebsocketIngestorPlan::endpoint_from_config(&[])
            .expect_err("missing websocket endpoint");

        assert!(
            error
                .to_string()
                .contains("missing WebSockets client config key 'endpoint'")
        );
    }
}
