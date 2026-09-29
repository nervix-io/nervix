//! WebSocket client source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** WebSocket client configuration, DNS-backed connections, signaling before source
//!   readiness, source frame delivery, and reconnect lifecycle operations.
//! - **Depends on.** The connector contract, compiled signaling engine, typed client
//!   configuration, the node resolver, Tokio, and WebSocket transport.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{collections::VecDeque, future::Future, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_connector::{
    BrokerSourceConnector, ClientConfigResult, IngestMessageHeaders, IngestMetadataRow,
    NoIngestHeaders, RustlsClientConfigSource, SourceBatch, SourceBatchRequest, SourceConnector,
    SourceError, SourceMessage, SourceResult, SourceResume, client_config_value,
};
use nervix_dns::{ConnectionBudget, DnsResolver};
use nervix_models::ClientConfigEntry;
use nervix_primitives::sync::mpsc;
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config, tungstenite::Message,
};
use triomphe::Arc;
use url::Url;

use crate::{CompiledSignalingProtocol, SignalingDataSink, WebsocketSignalingSession};

const WEBSOCKETS: &str = "websockets";
const CONNECT_BUDGET: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum WebsocketSourcePlanError {
    #[error("invalid WebSockets client configuration")]
    Configuration,
    #[error("invalid WebSockets endpoint")]
    Endpoint,
    #[error("unsupported WebSockets endpoint scheme '{scheme}', expected ws:// or wss://")]
    Scheme { scheme: String },
}

#[derive(Clone)]
pub struct WebsocketSourcePlan {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    entries: Vec<ClientConfigEntry>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
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

impl WebsocketSourcePlan {
    pub fn new(
        entries: Vec<ClientConfigEntry>,
        signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
        dns: DnsResolver,
    ) -> Result<Self, Report<WebsocketSourcePlanError>> {
        let endpoint = Self::endpoint_from_config(&entries)
            .change_context(WebsocketSourcePlanError::Configuration)?;
        let parsed = Url::parse(&endpoint).map_err(|error| {
            Report::new(WebsocketSourcePlanError::Endpoint).attach_printable(error)
        })?;
        let endpoint_requires_tls = match parsed.scheme() {
            "ws" => false,
            "wss" => true,
            scheme => {
                return Err(Report::new(WebsocketSourcePlanError::Scheme {
                    scheme: scheme.to_string(),
                }));
            }
        };
        let host = parsed
            .host_str()
            .ok_or_else(|| Report::new(WebsocketSourcePlanError::Endpoint))?;
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| Report::new(WebsocketSourcePlanError::Endpoint))?;
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
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint.raw
    }

    pub fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "WebSockets")
    }
}

pub struct WebsocketSourceMessage {
    payload: Vec<u8>,
    position: (),
    headers: NoIngestHeaders,
}

impl WebsocketSourceMessage {
    fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            position: (),
            headers: NoIngestHeaders,
        }
    }
}

impl SourceMessage for WebsocketSourceMessage {
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

pub struct WebsocketSource {
    endpoint: WebsocketEndpoint,
    dns: DnsResolver,
    tls_connector: Option<Connector>,
    signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
    relay: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending: VecDeque<Vec<u8>>,
}

#[async_trait]
impl SourceConnector for WebsocketSource {
    type Plan = WebsocketSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let tls_connector = if plan.endpoint.requires_tls {
            let config = RustlsClientConfigSource::new(&plan.entries)
                .build_with_default_roots()
                .change_context(SourceError::Open {
                    connector: WEBSOCKETS,
                })?;
            Some(Connector::Rustls(config))
        } else {
            None
        };
        Ok(Self {
            endpoint: plan.endpoint.clone(),
            dns: plan.dns.clone(),
            tls_connector,
            signaling_protocol: plan.signaling_protocol.clone(),
            relay: None,
            pending: VecDeque::new(),
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.relay.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        self.relay = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.relay.is_some() {
            return Ok(SourceResume::Ready);
        }
        let budget = ConnectionBudget::start(CONNECT_BUDGET);
        let addresses = self
            .dns
            .resolve(&self.endpoint.host, self.endpoint.port, budget.remaining())
            .await
            .change_context(SourceError::Resume {
                connector: WEBSOCKETS,
            })?;
        let mut report = Report::new(SourceError::Resume {
            connector: WEBSOCKETS,
        });
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
                Ok(Err(error)) => {
                    report = report.attach_printable(format!("{}: {error}", attempt.address));
                }
                Err(_) => {
                    report = report.attach_printable(format!(
                        "{}: no WebSocket connection within {:?}",
                        attempt.address, attempt.budget
                    ));
                }
            }
        }
        let Some((mut relay, _)) = connected else {
            return Err(report);
        };
        if let Some(protocol) = self.signaling_protocol.as_ref() {
            let (payloads_tx, mut payloads_rx) = mpsc::unbounded_channel();
            let sink = QueuedSignalingDataSink { payloads_tx };
            WebsocketSignalingSession::new(protocol.clone())
                .run(&mut relay, &sink)
                .await
                .map_err(|error| {
                    Report::new(SourceError::Resume {
                        connector: WEBSOCKETS,
                    })
                    .attach_printable(format!("websocket signaling failed: {error}"))
                })?;
            drop(sink);
            while let Ok(payload) = payloads_rx.try_recv() {
                self.pending.push_back(payload);
            }
        }
        self.relay = Some(relay);
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.relay = None;
        self.pending.clear();
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for WebsocketSource {
    type Message = WebsocketSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        if let Some(payload) = self.pending.pop_front() {
            return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                payload,
            )]));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(relay) = self.relay.as_mut() else {
                return Ok(SourceBatch::ResumeRequired);
            };
            let message = futures_util::StreamExt::next(relay).await;
            match message {
                Some(Ok(Message::Text(text))) => {
                    return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                        text.to_string().into_bytes(),
                    )]));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    return Ok(SourceBatch::Messages(vec![WebsocketSourceMessage::new(
                        bytes.to_vec(),
                    )]));
                }
                Some(Ok(Message::Close(_))) | None => {
                    self.relay = None;
                    return Ok(SourceBatch::ResumeRequired);
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                Some(Err(error)) => {
                    self.relay = None;
                    return Err(Report::new(SourceError::Read {
                        connector: WEBSOCKETS,
                    })
                    .attach_printable(error.to_string()));
                }
            }
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

struct QueuedSignalingDataSink {
    payloads_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl SignalingDataSink for QueuedSignalingDataSink {
    fn accept(&self, payload: Vec<u8>) -> impl Future<Output = ()> + Send {
        self.payloads_tx
            .send(payload)
            .assured("the signaling session retains its payload receiver while the sink is live");
        std::future::ready(())
    }
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, time::Duration};

    use meticulous::OptionExt as _;
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

    use super::*;

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
                "options ndots:1 timeout:1 attempts:1\n",
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
        let plan = WebsocketSourcePlan::new(endpoint(endpoint_url), None, fixture.resolver.clone())
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
        let mut source = WebsocketSource::open(&plan, 0)
            .await
            .assured("the source can open");
        assert_eq!(
            source
                .resume()
                .await
                .assured("the second address is reachable"),
            SourceResume::Ready
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
        let plan = WebsocketSourcePlan::new(
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
        let mut source = WebsocketSource::open(&plan, 0)
            .await
            .assured("the source can open");
        assert_eq!(
            source
                .resume()
                .await
                .assured("the IPv6 endpoint is reachable"),
            SourceResume::Ready
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
            let plan =
                WebsocketSourcePlan::new(endpoint(url.to_string()), None, fixture.resolver.clone())
                    .assured("the WebSocket URL is valid");
            assert_eq!(plan.endpoint.port, port);
            assert_eq!(plan.endpoint.requires_tls, requires_tls);
            assert_eq!(plan.endpoint.raw, url);
        }
        let error = WebsocketSourcePlan::new(
            endpoint("http://websocket.nervix.test/path".to_string()),
            None,
            fixture.resolver.clone(),
        )
        .err()
        .assured("HTTP is not a WebSocket endpoint");
        assert!(matches!(
            error.current_context(),
            WebsocketSourcePlanError::Scheme { .. }
        ));
    }

    #[test]
    fn source_plan_reads_its_endpoint_from_the_client_config() {
        let config = vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "wss://example.com/socket".to_string(),
        }];

        assert_eq!(
            WebsocketSourcePlan::endpoint_from_config(&config).expect("endpoint"),
            "wss://example.com/socket"
        );
    }

    #[test]
    fn source_plan_names_a_missing_endpoint_key() {
        let error =
            WebsocketSourcePlan::endpoint_from_config(&[]).expect_err("missing websocket endpoint");

        assert!(
            error
                .to_string()
                .contains("missing WebSockets client config key 'endpoint'")
        );
    }
}
