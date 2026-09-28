//! Syslog sink transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Syslog sender a client's transport configures, DNS-backed UDP, TCP and TLS
//!   connections, RFC 6587 stream framing, and the payload limits each transport enforces.
//! - **Depends on.** The connector contract, this crate's shared client configuration,
//!   the node resolver, `error-stack`, Tokio, `rustls` and `tokio-rustls`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::Report;
use nervix_connector::{
    PerRecordOutcome, RecordSink, SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult,
    SinkRecord, SinkRecordId, SinkStartError, SinkStartResult,
};
use nervix_dns::{ConnectionBudget, DnsResolver};
use nervix_models::ClientConfigEntry;
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
    time::timeout,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{
    MAX_UDP_PAYLOAD_SIZE, SyslogClientConfig, SyslogDirection, SyslogFraming, SyslogProtocol,
};

const SYSLOG: &str = "syslog";
const CONNECT_BUDGET: Duration = Duration::from_secs(30);

/// What one Syslog sink sends through: the entries its client declares its transport with.
pub struct SyslogSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

pub struct SyslogSink {
    config: SyslogClientConfig,
    sender: SyslogSender,
}

enum SyslogSender {
    Udp(UdpSocket),
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

#[derive(Debug, Error)]
enum SyslogPayloadError {
    #[error("encoded Syslog UDP payload is {size} bytes; maximum is {maximum}")]
    OversizedUdp { size: usize, maximum: usize },
    #[error(
        "encoded Syslog payload is {size} bytes; RFC 6587 octet count permits at most {maximum}"
    )]
    OversizedOctetCount { size: usize, maximum: usize },
    #[error(
        "encoded Syslog payload contains LF, which is not allowed with non-transparent framing"
    )]
    NonTransparentLf,
}

impl SyslogSink {
    pub async fn new(config: SyslogSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let client = Self::client_config(&config.config)?;
        let sender = Self::connect(&client, &config.dns).await?;
        Ok(Self {
            config: client,
            sender,
        })
    }

    /// Checks that `config` declares a usable Syslog transport, loading its TLS material when the
    /// transport is TLS, so an unusable configuration fails the emitter's start instead of every
    /// connection attempt.
    pub fn check_client_config(config: &[ClientConfigEntry]) -> SinkStartResult<()> {
        let config = Self::client_config(config)?;
        if config.protocol == SyslogProtocol::Tls {
            config.tls_client_config().map_err(Self::config_error)?;
        }
        Ok(())
    }

    /// The Syslog transport a resolved client configuration declares.
    fn client_config(config: &[ClientConfigEntry]) -> SinkStartResult<SyslogClientConfig> {
        SyslogClientConfig::parse(config, SyslogDirection::Emit).map_err(Self::config_error)
    }

    async fn connect(
        config: &SyslogClientConfig,
        dns: &DnsResolver,
    ) -> SinkStartResult<SyslogSender> {
        let budget = ConnectionBudget::start(CONNECT_BUDGET);
        let addresses = dns
            .resolve(&config.server_name, config.port, budget.remaining())
            .await
            .map_err(|error| {
                let reason = error.current_context().to_string();
                error
                    .change_context(SinkStartError::Initialize { sink: SYSLOG })
                    .attach_printable(reason)
            })?;
        match config.protocol {
            SyslogProtocol::Udp => {
                let mut report = Report::new(SinkStartError::Initialize { sink: SYSLOG });
                for attempt in budget.attempts(&addresses) {
                    tokio::task::consume_budget().await;
                    let local = SocketAddr::new(
                        match attempt.address.ip() {
                            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                        },
                        0,
                    );
                    let connected = timeout(attempt.budget, async {
                        let socket = UdpSocket::bind(local).await?;
                        socket.connect(attempt.address).await?;
                        Ok::<UdpSocket, std::io::Error>(socket)
                    })
                    .await;
                    match connected {
                        Ok(Ok(socket)) => return Ok(SyslogSender::Udp(socket)),
                        Ok(Err(error)) => {
                            report =
                                report.attach_printable(format!("{}: {error}", attempt.address));
                        }
                        Err(_) => {
                            report = report.attach_printable(format!(
                                "{}: no UDP setup within {:?}",
                                attempt.address, attempt.budget
                            ));
                        }
                    }
                }
                Err(report)
            }
            SyslogProtocol::Tcp => {
                let mut report = Report::new(SinkStartError::Initialize { sink: SYSLOG });
                for attempt in budget.attempts(&addresses) {
                    tokio::task::consume_budget().await;
                    let stream = timeout(attempt.budget, TcpStream::connect(attempt.address)).await;
                    match stream {
                        Ok(Ok(stream)) => {
                            stream.set_nodelay(true).map_err(Self::start_error)?;
                            return Ok(SyslogSender::Tcp(stream));
                        }
                        Ok(Err(error)) => {
                            report =
                                report.attach_printable(format!("{}: {error}", attempt.address));
                        }
                        Err(_) => {
                            report = report.attach_printable(format!(
                                "{}: no connection within {:?}",
                                attempt.address, attempt.budget
                            ));
                        }
                    }
                }
                Err(report)
            }
            SyslogProtocol::Tls => {
                let server_name =
                    ServerName::try_from(config.server_name.clone()).map_err(|error| {
                        Self::config_error(format!(
                            "invalid Syslog client config key 'addr' TLS server name '{}': {error}",
                            config.server_name
                        ))
                    })?;
                let connector =
                    TlsConnector::from(config.tls_client_config().map_err(Self::config_error)?);
                let mut report = Report::new(SinkStartError::Initialize { sink: SYSLOG });
                for attempt in budget.attempts(&addresses) {
                    tokio::task::consume_budget().await;
                    let connected = timeout(attempt.budget, async {
                        let stream = TcpStream::connect(attempt.address).await?;
                        stream.set_nodelay(true)?;
                        connector.connect(server_name.clone(), stream).await
                    })
                    .await;
                    match connected {
                        Ok(Ok(stream)) => return Ok(SyslogSender::Tls(Box::new(stream))),
                        Ok(Err(error)) => {
                            report =
                                report.attach_printable(format!("{}: {error}", attempt.address));
                        }
                        Err(_) => {
                            report = report.attach_printable(format!(
                                "{}: no TLS connection within {:?}",
                                attempt.address, attempt.budget
                            ));
                        }
                    }
                }
                Err(report)
            }
        }
    }

    fn validate_payload(&self, payload: &[u8]) -> error_stack::Result<(), SyslogPayloadError> {
        if self.config.protocol == SyslogProtocol::Udp && payload.len() > MAX_UDP_PAYLOAD_SIZE {
            return Err(Report::new(SyslogPayloadError::OversizedUdp {
                size: payload.len(),
                maximum: MAX_UDP_PAYLOAD_SIZE,
            }));
        }
        if self.config.protocol == SyslogProtocol::Tcp
            && self.config.framing == SyslogFraming::NonTransparent
            && payload.contains(&b'\n')
        {
            return Err(Report::new(SyslogPayloadError::NonTransparentLf));
        }
        if self.config.protocol != SyslogProtocol::Udp
            && self.config.framing == SyslogFraming::OctetCounting
        {
            Self::validate_octet_count_size(payload.len())?;
        }
        Ok(())
    }

    fn validate_octet_count_size(size: usize) -> error_stack::Result<(), SyslogPayloadError> {
        const MAX_OCTET_COUNT: usize = 9_999_999_999;
        if size > MAX_OCTET_COUNT {
            return Err(Report::new(SyslogPayloadError::OversizedOctetCount {
                size,
                maximum: MAX_OCTET_COUNT,
            }));
        }
        Ok(())
    }

    async fn publish_payload(&mut self, payload: &[u8]) -> SinkPublishResult<()> {
        match &mut self.sender {
            SyslogSender::Udp(socket) => {
                let written = socket.send(payload).await.map_err(Self::publish_error)?;
                if written != payload.len() {
                    return Err(Self::publish_error(format!(
                        "Syslog UDP socket accepted {written} of {} bytes",
                        payload.len()
                    )));
                }
            }
            SyslogSender::Tcp(stream) => {
                write_stream_frame(stream, self.config.framing, payload).await?;
            }
            SyslogSender::Tls(stream) => {
                write_stream_frame(stream.as_mut(), SyslogFraming::OctetCounting, payload).await?;
            }
        }
        Ok(())
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: SYSLOG })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: SYSLOG }).attach_printable(error.to_string())
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: SYSLOG }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for SyslogSink {}

#[async_trait]
impl RecordSink for SyslogSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        // A socket that accepted a frame has not delivered it, so a write failure leaves every
        // record of this write for retry, including records whose frames were already accepted
        // and may therefore be delivered twice.
        let mut staged_deliveries = Vec::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            if let Err(reason) = self.validate_payload(&record.payload) {
                outcome.reject(record.rejected(reason.to_string()));
                continue;
            }
            match self.publish_payload(&record.payload).await {
                Ok(()) => staged_deliveries.push(record.id),
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            }
        }
        for record in staged_deliveries {
            outcome.deliver(record);
        }
        outcome
    }
}

async fn write_stream_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    framing: SyslogFraming,
    payload: &[u8],
) -> SinkPublishResult<()> {
    match framing {
        SyslogFraming::OctetCounting => {
            let prefix = format!("{} ", payload.len());
            stream
                .write_all(prefix.as_bytes())
                .await
                .map_err(SyslogSink::publish_error)?;
            stream
                .write_all(payload)
                .await
                .map_err(SyslogSink::publish_error)?;
        }
        SyslogFraming::NonTransparent => {
            stream
                .write_all(payload)
                .await
                .map_err(SyslogSink::publish_error)?;
            stream
                .write_all(b"\n")
                .await
                .map_err(SyslogSink::publish_error)?;
        }
    }
    stream.flush().await.map_err(SyslogSink::publish_error)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt as _;

    use super::*;

    fn config(protocol: &str, framing: Option<&str>) -> SyslogClientConfig {
        let mut entries = vec![
            ClientConfigEntry {
                key: "protocol".to_string(),
                value: protocol.to_string(),
            },
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "127.0.0.1:5514".to_string(),
            },
        ];
        if let Some(framing) = framing {
            entries.push(ClientConfigEntry {
                key: "framing".to_string(),
                value: framing.to_string(),
            });
        }
        SyslogClientConfig::parse(&entries, SyslogDirection::Emit)
            .expect("test Syslog emitter config must parse")
    }

    async fn sink(config: SyslogClientConfig) -> SyslogSink {
        let receiver = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test UDP receiver must bind");
        let sender = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test UDP sender must bind");
        sender
            .connect(
                receiver
                    .local_addr()
                    .expect("receiver must have an address"),
            )
            .await
            .expect("test UDP sender must connect");
        SyslogSink {
            config,
            sender: SyslogSender::Udp(sender),
        }
    }

    struct DnsFixture {
        authority: DnsAuthority,
        resolver: DnsResolver,
        _files: TempDir,
    }

    impl DnsFixture {
        async fn start(hosts: &str) -> Self {
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
            std::fs::write(&hosts_file, hosts).assured("the fixture hosts file can be written");
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
    }

    fn endpoint_config(protocol: &str, addr: &str) -> SyslogClientConfig {
        SyslogClientConfig::parse(
            &[
                ClientConfigEntry {
                    key: "protocol".to_string(),
                    value: protocol.to_string(),
                },
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: addr.to_string(),
                },
            ],
            SyslogDirection::Emit,
        )
        .assured("the test endpoint is a valid Syslog client")
    }

    #[tokio::test]
    async fn tcp_tries_dns_answers_in_order_and_writes_to_the_reachable_address() {
        let fixture = DnsFixture::start("").await;
        let listener = tokio::net::TcpListener::bind("127.0.7.2:0")
            .await
            .assured("a loopback TCP port is available");
        let port = listener
            .local_addr()
            .assured("a bound listener has a local address")
            .port();
        fixture.authority.set(
            "syslog.nervix.test",
            DnsAnswer::Addresses {
                addresses: vec![
                    "127.0.7.1".parse().assured("literal IPv4 address"),
                    "127.0.7.2".parse().assured("literal IPv4 address"),
                ],
                ttl: Duration::from_secs(1),
            },
        );
        let config = endpoint_config("tcp", &format!("syslog.nervix.test:{port}"));
        let SyslogSender::Tcp(mut sender) = SyslogSink::connect(&config, &fixture.resolver)
            .await
            .assured("the second DNS answer has a listener")
        else {
            panic!("the TCP configuration must open a TCP stream");
        };
        write_stream_frame(&mut sender, SyslogFraming::OctetCounting, b"hello")
            .await
            .assured("the connected Syslog stream accepts the frame");
        let (mut received, _) = listener
            .accept()
            .await
            .assured("the second address was dialled");
        let mut frame = [0_u8; 7];
        received
            .read_exact(&mut frame)
            .await
            .assured("the entire Syslog frame arrives");
        assert_eq!(&frame, b"5 hello");
    }

    #[tokio::test]
    async fn udp_literal_ipv6_uses_an_ipv6_socket() {
        let fixture = DnsFixture::start("").await;
        let receiver = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))
            .await
            .assured("the IPv6 loopback address is available");
        let address = receiver.local_addr().assured("the UDP receiver is bound");
        let config = endpoint_config("udp", &address.to_string());
        let SyslogSender::Udp(sender) = SyslogSink::connect(&config, &fixture.resolver)
            .await
            .assured("the literal IPv6 address can be connected")
        else {
            panic!("the UDP configuration must open a UDP socket");
        };
        assert!(
            sender
                .local_addr()
                .assured("the UDP sender is bound")
                .is_ipv6()
        );
        sender
            .send(b"ipv6")
            .await
            .assured("the UDP payload can be sent");
        let mut payload = [0_u8; 4];
        let count = receiver
            .recv(&mut payload)
            .await
            .assured("the receiver gets the payload");
        assert_eq!(count, payload.len());
        assert_eq!(&payload, b"ipv6");
    }

    #[tokio::test]
    async fn tcp_uses_the_hosts_file_before_dns() {
        let fixture = DnsFixture::start("127.0.7.2 listed.nervix.test\n").await;
        let listener = tokio::net::TcpListener::bind("127.0.7.2:0")
            .await
            .assured("a loopback TCP port is available");
        let port = listener
            .local_addr()
            .assured("the listener is bound")
            .port();
        let config = endpoint_config("tcp", &format!("listed.nervix.test:{port}"));
        let sender = SyslogSink::connect(&config, &fixture.resolver)
            .await
            .assured("the hosts-file address has a listener");
        assert!(matches!(sender, SyslogSender::Tcp(_)));
        listener
            .accept()
            .await
            .assured("the hosts-file address was dialled");
        assert_eq!(fixture.authority.questions_for("listed.nervix.test"), 0);
    }

    #[tokio::test]
    async fn missing_name_is_an_initialization_failure_with_the_dns_cause() {
        let fixture = DnsFixture::start("").await;
        fixture.authority.set(
            "missing.nervix.test",
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let config = endpoint_config("tcp", "missing.nervix.test:6514");
        let failure = SyslogSink::connect(&config, &fixture.resolver)
            .await
            .err()
            .assured("the fixture name has no address");
        assert!(matches!(
            failure.current_context(),
            SinkStartError::Initialize { sink: SYSLOG }
        ));
        assert!(format!("{failure:?}").contains("the name does not exist"));
    }

    #[tokio::test]
    async fn stream_writer_emits_both_rfc6587_framings() {
        for (framing, expected) in [
            (SyslogFraming::OctetCounting, b"5 hello".as_slice()),
            (SyslogFraming::NonTransparent, b"hello\n".as_slice()),
        ] {
            let (mut writer, mut reader) = tokio::io::duplex(64);
            write_stream_frame(&mut writer, framing, b"hello")
                .await
                .expect("Syslog stream frame must write");
            drop(writer);
            let mut actual = Vec::new();
            reader
                .read_to_end(&mut actual)
                .await
                .expect("Syslog stream frame must be readable");
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn sink_rejects_udp_oversize_and_non_transparent_lf() {
        let udp = sink(config("udp", None)).await;
        let oversized = vec![0_u8; MAX_UDP_PAYLOAD_SIZE + 1];
        let maximum = vec![0_u8; MAX_UDP_PAYLOAD_SIZE];
        assert!(udp.validate_payload(&oversized).is_err());
        assert!(udp.validate_payload(&maximum).is_ok());

        let tcp = sink(config("tcp", Some("non-transparent"))).await;
        assert!(tcp.validate_payload(b"line one\nline two").is_err());
        assert!(tcp.validate_payload(b"one line").is_ok());
    }

    #[test]
    fn octet_count_prefix_has_at_most_ten_digits() {
        assert!(SyslogSink::validate_octet_count_size(9_999_999_999).is_ok());
        assert!(SyslogSink::validate_octet_count_size(10_000_000_000).is_err());
    }
}
