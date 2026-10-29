//! Syslog emitter transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Syslog sender a client's transport configures, DNS-backed UDP, TCP and TLS
//!   connections opened within the client's `connect_timeout_ms`, frame writes bounded by its
//!   `timeout_ms`, RFC 6587 stream framing, and the payload limits each transport enforces.
//! - **Depends on.** The connector contract, this crate's shared client configuration,
//!   the node resolver, `error-stack`, Tokio, `rustls` and `tokio-rustls`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

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

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError,
    EmitterWriteResult, PerRecordOutcome, RecordEmitter,
};
use nervix_dns::{DnsResolver, LookupShare, OperationBudget};
use nervix_models::{ClientConfigEntry, SYSLOG_FRAME_LIMITS};
use nervix_primitives::{
    net::{TcpStream, UdpSocket},
    time::{Instant, timeout},
};
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{SyslogClientConfig, SyslogDirection, SyslogFraming, SyslogProtocol};

const SYSLOG: &str = "syslog";

/// What one Syslog emitter sends through: the entries its client declares its transport with.
pub struct SyslogCollectorEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

pub struct SyslogCollectorEmitter {
    config: SyslogClientConfig,
    sender: SyslogSender,
    /// The client's `timeout_ms`, which bounds writing and flushing each frame, where it sets one.
    write_timeout: Option<ConfiguredDeadline>,
}

enum SyslogSender {
    Udp(UdpSocket),
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

#[derive(Debug, Error)]
enum SyslogPayloadError {
    #[error("encoded Syslog UDP payload is {size} bytes; maximum is {maximum}")]
    OversizedUdp { size: u64, maximum: u64 },
    #[error(
        "encoded Syslog payload is {size} bytes; RFC 6587 octet count permits at most {maximum}"
    )]
    OversizedOctetCount { size: u64, maximum: u64 },
    #[error(
        "encoded Syslog payload contains LF, which is not allowed with non-transparent framing"
    )]
    NonTransparentLf,
}

impl SyslogCollectorEmitter {
    pub async fn new(
        config: SyslogCollectorEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let client = Self::client_config(&config.config)?;
        let connect_timeout = Self::connect_timeout(&config.config)?;
        let write_timeout = Self::write_timeout(&config.config)?;
        let sender = Self::connect(&client, connect_timeout, &config.dns).await?;
        Ok(Self {
            config: client,
            sender,
            write_timeout,
        })
    }

    /// Checks that `config` declares a usable Syslog transport and deadlines, loading its TLS
    /// material when the transport is TLS, so an unusable configuration fails the emitter's start
    /// instead of every connection attempt.
    pub fn check_client_config(config: &[ClientConfigEntry]) -> EmitterStartResult<()> {
        let client = Self::client_config(config)?;
        Self::connect_timeout(config)?;
        Self::write_timeout(config)?;
        if client.protocol == SyslogProtocol::Tls {
            client.tls_client_config().map_err(Self::config_error)?;
        }
        Ok(())
    }

    /// The deadline the client's `connect_timeout_ms` sets on opening the emitter's connection.
    fn connect_timeout(config: &[ClientConfigEntry]) -> EmitterStartResult<ConfiguredDeadline> {
        ConfiguredDeadline::connect(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: SYSLOG })
    }

    /// The deadline the client's `timeout_ms` sets on writing and flushing each frame, where it
    /// sets one.
    fn write_timeout(
        config: &[ClientConfigEntry],
    ) -> EmitterStartResult<Option<ConfiguredDeadline>> {
        ConfiguredDeadline::timeout(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: SYSLOG })
    }

    /// Why no connection was opened, with each address attempt's failure. When the attempts ran
    /// out of the client's `connect_timeout_ms`, the failure names it beneath the emitter's, and its
    /// message is attached last, so it leads the report the emitter shows.
    fn unconnected(
        budget: &OperationBudget,
        connect_timeout: ConfiguredDeadline,
        step: &'static str,
        failures: Vec<String>,
    ) -> Report<EmitterOpenError> {
        let initialize = EmitterOpenError::Initialize { connector: SYSLOG };
        let elapsed = if budget.remaining().is_zero() {
            Some(connect_timeout.elapsed(step))
        } else {
            None
        };
        let mut report = match elapsed {
            Some(elapsed) => Report::new(elapsed).change_context(initialize),
            None => Report::new(initialize),
        };
        for failure in failures {
            report = report.attach_printable(failure);
        }
        if let Some(elapsed) = elapsed {
            report = report.attach_printable(elapsed.to_string());
        }
        report
    }

    /// The Syslog transport a resolved client configuration declares.
    fn client_config(config: &[ClientConfigEntry]) -> EmitterStartResult<SyslogClientConfig> {
        SyslogClientConfig::parse(config, SyslogDirection::Emit).map_err(Self::config_error)
    }

    async fn connect(
        config: &SyslogClientConfig,
        connect_timeout: ConfiguredDeadline,
        dns: &DnsResolver,
    ) -> EmitterStartResult<SyslogSender> {
        let budget = OperationBudget::start(connect_timeout.bound(), LookupShare::Whole);
        let addresses = {
            let mapping_input = dns
                .resolve(&config.server_name, config.port, budget.lookup())
                .await;
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let reason = error.current_context().to_string();
                    error
                        .change_context(EmitterOpenError::Initialize { connector: SYSLOG })
                        .attach_printable(reason)
                }),
            }
        }?;
        match config.protocol {
            SyslogProtocol::Udp => {
                let mut failures = Vec::new();
                for attempt in budget.attempts(&addresses) {
                    nervix_primitives::task::consume_budget().await;
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
                        Ok(Err(error)) => failures.push(format!("{}: {error}", attempt.address)),
                        Err(_) => failures.push(format!(
                            "{}: no UDP setup within {:?}",
                            attempt.address, attempt.budget
                        )),
                    }
                }
                Err(Self::unconnected(
                    &budget,
                    connect_timeout,
                    "setting up the Syslog UDP socket",
                    failures,
                ))
            }
            SyslogProtocol::Tcp => {
                let mut failures = Vec::new();
                for attempt in budget.attempts(&addresses) {
                    nervix_primitives::task::consume_budget().await;
                    let stream = timeout(attempt.budget, TcpStream::connect(attempt.address)).await;
                    match stream {
                        Ok(Ok(stream)) => {
                            stream.set_nodelay(true).map_err(Self::start_error)?;
                            return Ok(SyslogSender::Tcp(stream));
                        }
                        Ok(Err(error)) => failures.push(format!("{}: {error}", attempt.address)),
                        Err(_) => failures.push(format!(
                            "{}: no connection within {:?}",
                            attempt.address, attempt.budget
                        )),
                    }
                }
                Err(Self::unconnected(
                    &budget,
                    connect_timeout,
                    "connecting to the Syslog server",
                    failures,
                ))
            }
            SyslogProtocol::Tls => {
                let server_name =
                    ServerName::try_from(config.server_name.clone()).map_err(|error| {
                        Report::new(EmitterOpenError::InvalidConfiguration { connector: SYSLOG })
                            .attach_printable(format!(
                                "invalid Syslog client config key 'addr' TLS server name '{}': \
                                 {error}",
                                config.server_name
                            ))
                    })?;
                let connector =
                    TlsConnector::from(config.tls_client_config().map_err(Self::config_error)?);
                let mut failures = Vec::new();
                for attempt in budget.attempts(&addresses) {
                    nervix_primitives::task::consume_budget().await;
                    let connected = timeout(attempt.budget, async {
                        let stream = TcpStream::connect(attempt.address).await?;
                        stream.set_nodelay(true)?;
                        connector.connect(server_name.clone(), stream).await
                    })
                    .await;
                    match connected {
                        Ok(Ok(stream)) => return Ok(SyslogSender::Tls(Box::new(stream))),
                        Ok(Err(error)) => failures.push(format!("{}: {error}", attempt.address)),
                        Err(_) => failures.push(format!(
                            "{}: no TLS connection within {:?}",
                            attempt.address, attempt.budget
                        )),
                    }
                }
                Err(Self::unconnected(
                    &budget,
                    connect_timeout,
                    "the TLS connection to the Syslog server",
                    failures,
                ))
            }
        }
    }

    /// Refuses a payload no frame of the client's transport carries, as the declared frame limits
    /// state them.
    fn validate_payload(&self, payload: &[u8]) -> error_stack::Result<(), SyslogPayloadError> {
        let size: u64 = payload.len().arch_into();
        let datagram_payload = SYSLOG_FRAME_LIMITS.datagram_payload();
        if self.config.protocol == SyslogProtocol::Udp && size > datagram_payload {
            return Err(Report::new(SyslogPayloadError::OversizedUdp {
                size,
                maximum: datagram_payload,
            }));
        }
        if self.config.protocol == SyslogProtocol::Tcp
            && self.config.framing == SyslogFraming::NonTransparent
            && payload.contains(&SYSLOG_FRAME_LIMITS.non_transparent_trailer())
        {
            return Err(Report::new(SyslogPayloadError::NonTransparentLf));
        }
        if self.config.protocol != SyslogProtocol::Udp
            && self.config.framing == SyslogFraming::OctetCounting
        {
            Self::validate_octet_count_size(size)?;
        }
        Ok(())
    }

    fn validate_octet_count_size(size: u64) -> error_stack::Result<(), SyslogPayloadError> {
        let maximum = SYSLOG_FRAME_LIMITS.largest_octet_count();
        if size > maximum {
            return Err(Report::new(SyslogPayloadError::OversizedOctetCount {
                size,
                maximum,
            }));
        }
        Ok(())
    }

    /// Writes one frame, and fails the write once it has taken longer than the client's
    /// `timeout_ms`: a server that stops reading fills the connection's buffers and holds the
    /// write. The host rebuilds the connection after a failed publish, so the frame a timed-out
    /// write left partly written is never continued.
    async fn publish_payload(&mut self, payload: &[u8]) -> EmitterWriteResult<()> {
        let write_timeout = self.write_timeout;
        let write = self.write_frame(payload);
        let Some(deadline) = write_timeout else {
            return write.await;
        };
        match timeout(deadline.bound(), write).await {
            Ok(written) => written,
            Err(_) => Err(Self::write_elapsed(deadline)),
        }
    }

    /// The failure of a frame write that ran out of the client's `timeout_ms`, with the elapsed
    /// deadline beneath it. Its message is attached last, so it leads the report the emitter shows.
    fn write_elapsed(deadline: ConfiguredDeadline) -> Report<EmitterWriteError> {
        let elapsed = deadline.elapsed("writing a frame to the Syslog server");
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(EmitterWriteError::Publish { connector: SYSLOG })
            .attach_printable(message)
    }

    async fn write_frame(&mut self, payload: &[u8]) -> EmitterWriteResult<()> {
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

    fn config_error(error: Report<crate::config::SyslogConfigError>) -> Report<EmitterOpenError> {
        // A configuration value that does not parse keeps its parser's error as the frame beneath
        // it, so the reason is the whole chain.
        let reason = format!("{error:#}");
        error
            .change_context(EmitterOpenError::InvalidConfiguration { connector: SYSLOG })
            .attach_printable(reason)
    }

    fn start_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::Initialize { connector: SYSLOG })
            .attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the generic external writer and driver error formatter own \
                                   their effects; local callbacks remain analyzed")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: SYSLOG })
            .attach_printable(error.to_string())
    }
}

impl SyslogCollectorEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Syslog(nervix_models::SyslogEmitterEntity::Collector);
}

#[async_trait]
impl EmitterLifecycle for SyslogCollectorEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each frame is written and flushed to the socket before the publish returns, so the
    /// sender holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for SyslogCollectorEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        // A socket that accepted a frame has not delivered it, so a write failure leaves every
        // record of this write for retry, including records whose frames were already accepted
        // and may therefore be delivered twice.
        let mut staged_deliveries = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
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

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the generic external writer and driver error formatter own their effects; local \
                  callbacks remain analyzed"
    )
)]
async fn write_stream_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    framing: SyslogFraming,
    payload: &[u8],
) -> EmitterWriteResult<()> {
    match framing {
        SyslogFraming::OctetCounting => {
            let prefix = format!("{} ", payload.len());
            stream
                .write_all(prefix.as_bytes())
                .await
                .map_err(SyslogCollectorEmitter::publish_error)?;
            stream
                .write_all(payload)
                .await
                .map_err(SyslogCollectorEmitter::publish_error)?;
        }
        SyslogFraming::NonTransparent => {
            stream
                .write_all(payload)
                .await
                .map_err(SyslogCollectorEmitter::publish_error)?;
            stream
                .write_all(b"\n")
                .await
                .map_err(SyslogCollectorEmitter::publish_error)?;
        }
    }
    stream
        .flush()
        .await
        .map_err(SyslogCollectorEmitter::publish_error)
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{net::Ipv6Addr, time::Duration};

    use error_stack::{AttachmentKind, FrameKind};
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_connector::DeadlineElapsed;
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_models::DeadlineKey;
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt as _;

    use super::*;
    use crate::SyslogConfigError;

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

    #[test]
    fn startup_keeps_the_typed_syslog_configuration_failure() {
        let failure = SyslogClientConfig::parse(&[], SyslogDirection::Emit)
            .expect_err("a client needs a protocol");
        let report = SyslogCollectorEmitter::config_error(failure);
        assert!(report.contains::<SyslogConfigError>());
        assert_eq!(
            report.current_context(),
            &EmitterOpenError::InvalidConfiguration { connector: SYSLOG }
        );
    }

    #[test]
    fn startup_describes_a_value_that_does_not_parse_with_its_parser_error() {
        let entries = [
            ("protocol", "tcp"),
            ("addr", "127.0.0.1:5514"),
            ("max_message_size", "many"),
        ]
        .into_iter()
        .map(|(key, value)| ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect::<Vec<_>>();
        let failure = SyslogClientConfig::parse(&entries, SyslogDirection::Emit)
            .expect_err("a message size must be a positive integer");

        let report = SyslogCollectorEmitter::config_error(failure);

        let mut descriptions = Vec::new();
        for frame in report.frames() {
            if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
                descriptions.push(attachment.to_string());
            }
        }
        assert_eq!(
            descriptions,
            [
                "invalid Syslog client config key 'max_message_size' value 'many': invalid digit \
                 found in string"
            ]
        );
    }

    async fn connector(config: SyslogClientConfig) -> SyslogCollectorEmitter {
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
        SyslogCollectorEmitter {
            config,
            sender: SyslogSender::Udp(sender),
            write_timeout: None,
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
                "search --\noptions ndots:1 timeout:1 attempts:1\n",
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

    /// The deadline an emitter's client that leaves `connect_timeout_ms` out connects within.
    fn default_connect_timeout() -> ConfiguredDeadline {
        ConfiguredDeadline::connect(&[]).assured("a client without the key takes the default")
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

    #[nervix_primitives::test]
    async fn tcp_tries_dns_answers_in_order_and_writes_to_the_reachable_address() {
        let fixture = DnsFixture::start("").await;
        let listener = nervix_primitives::net::TcpListener::bind("127.0.7.2:0")
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
        let SyslogSender::Tcp(mut sender) =
            SyslogCollectorEmitter::connect(&config, default_connect_timeout(), &fixture.resolver)
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

    #[nervix_primitives::test]
    async fn udp_literal_ipv6_uses_an_ipv6_socket() {
        let fixture = DnsFixture::start("").await;
        let receiver = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))
            .await
            .assured("the IPv6 loopback address is available");
        let address = receiver.local_addr().assured("the UDP receiver is bound");
        let config = endpoint_config("udp", &address.to_string());
        let SyslogSender::Udp(sender) =
            SyslogCollectorEmitter::connect(&config, default_connect_timeout(), &fixture.resolver)
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

    #[nervix_primitives::test]
    async fn tcp_uses_the_hosts_file_before_dns() {
        let fixture = DnsFixture::start("127.0.7.2 listed.nervix.test\n").await;
        let listener = nervix_primitives::net::TcpListener::bind("127.0.7.2:0")
            .await
            .assured("a loopback TCP port is available");
        let port = listener
            .local_addr()
            .assured("the listener is bound")
            .port();
        let config = endpoint_config("tcp", &format!("listed.nervix.test:{port}"));
        let sender =
            SyslogCollectorEmitter::connect(&config, default_connect_timeout(), &fixture.resolver)
                .await
                .assured("the hosts-file address has a listener");
        assert!(matches!(sender, SyslogSender::Tcp(_)));
        listener
            .accept()
            .await
            .assured("the hosts-file address was dialled");
        assert_eq!(fixture.authority.questions_for("listed.nervix.test"), 0);
    }

    #[nervix_primitives::test]
    async fn missing_name_is_an_initialization_failure_with_the_dns_cause() {
        let fixture = DnsFixture::start("").await;
        fixture.authority.set(
            "missing.nervix.test",
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let config = endpoint_config("tcp", "missing.nervix.test:6514");
        let failure =
            SyslogCollectorEmitter::connect(&config, default_connect_timeout(), &fixture.resolver)
                .await
                .err()
                .assured("the fixture name has no address");
        assert!(matches!(
            failure.current_context(),
            EmitterOpenError::Initialize { connector: SYSLOG }
        ));
        assert!(format!("{failure:?}").contains("the name does not exist"));
    }

    #[nervix_primitives::test]
    async fn a_tls_server_that_never_answers_is_reported_within_connect_timeout() {
        let fixture = DnsFixture::start("").await;
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        // Accepts the connection and never answers the TLS client hello.
        let silent = nervix_primitives::task::spawn(async move {
            let (held, _) = listener.accept().await.assured("the listener accepts it");
            std::future::pending::<()>().await;
            drop(held);
        });
        let entries = [
            ClientConfigEntry {
                key: "protocol".to_string(),
                value: "tls".to_string(),
            },
            ClientConfigEntry {
                key: "addr".to_string(),
                value: address.to_string(),
            },
            ClientConfigEntry {
                key: "connect_timeout_ms".to_string(),
                value: "200".to_string(),
            },
        ];
        let config = SyslogClientConfig::parse(&entries, SyslogDirection::Emit)
            .assured("the test endpoint is a valid Syslog client");
        let connect_timeout =
            SyslogCollectorEmitter::connect_timeout(&entries).assured("200 ms is a valid deadline");
        let failure = nervix_primitives::time::timeout(
            Duration::from_secs(10),
            SyslogCollectorEmitter::connect(&config, connect_timeout, &fixture.resolver),
        )
        .await
        .assured("connect_timeout_ms bounds the connection")
        .err()
        .assured("a server that never answers cannot be connected");
        assert!(matches!(
            failure.current_context(),
            EmitterOpenError::Initialize { connector: SYSLOG }
        ));
        let elapsed = failure
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .assured("the elapsed deadline stays beneath the start failure");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));
        assert!(format!("{failure:#}").contains("within connect_timeout_ms 200ms"));
        // The emitter shows the last message an emitter attached, and the deadline leads.
        let leading = failure
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>())
            .assured("the start failure carries a message");
        assert_eq!(
            leading,
            "the TLS connection to the Syslog server did not finish within connect_timeout_ms \
             200ms"
        );
        silent.abort();
    }

    #[test]
    fn an_invalid_deadline_fails_the_client_check() {
        for key in ["connect_timeout_ms", "timeout_ms"] {
            for value in ["0", "1s", ""] {
                let entries = [
                    ClientConfigEntry {
                        key: "protocol".to_string(),
                        value: "tcp".to_string(),
                    },
                    ClientConfigEntry {
                        key: "addr".to_string(),
                        value: "127.0.0.1:6514".to_string(),
                    },
                    ClientConfigEntry {
                        key: key.to_string(),
                        value: value.to_string(),
                    },
                ];
                let failure = SyslogCollectorEmitter::check_client_config(&entries)
                    .err()
                    .assured("an invalid deadline fails the check");
                assert!(
                    matches!(
                        failure.current_context(),
                        EmitterOpenError::InvalidConfiguration { connector: SYSLOG }
                    ),
                    "{key} {value:?}: {failure:?}"
                );
            }
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_server_that_stops_reading_fails_the_frame_write_at_timeout_ms() {
        let server = nervix_primitives::net::TcpSocket::new_v4()
            .assured("an IPv4 TCP socket can be created");
        server
            .set_recv_buffer_size(4_096)
            .assured("a TCP socket takes a receive buffer size");
        server
            .bind("127.0.0.1:0".parse().assured("literal socket address"))
            .assured("a loopback TCP port is available");
        let server = server.listen(1).assured("a bound socket listens");
        let address = server.local_addr().assured("the listener is bound");
        let client = nervix_primitives::net::TcpSocket::new_v4()
            .assured("an IPv4 TCP socket can be created");
        client
            .set_send_buffer_size(4_096)
            .assured("a TCP socket takes a send buffer size");
        let stream = client
            .connect(address)
            .await
            .assured("the listener accepts connections");
        // The server accepts the connection and never reads from it.
        let (_unread, _) = server.accept().await.assured("the connection arrives");
        let write_timeout = ConfiguredDeadline::timeout(&[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "2000".to_string(),
        }])
        .assured("two seconds is a valid deadline")
        .assured("the entry sets timeout_ms");
        let mut connector = SyslogCollectorEmitter {
            config: endpoint_config("tcp", &address.to_string()),
            sender: SyslogSender::Tcp(stream),
            write_timeout: Some(write_timeout),
        };
        let started = Instant::now();

        let failure = connector
            .publish_payload(&vec![b'x'; 4 * 1024 * 1024])
            .await
            .err()
            .assured("a server that reads nothing never takes the whole frame");

        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!(matches!(
            failure.current_context(),
            EmitterWriteError::Publish { connector: SYSLOG }
        ));
        let elapsed = failure
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .assured("the elapsed deadline stays beneath the publish failure");
        assert_eq!(elapsed.key, DeadlineKey::Timeout);
        assert_eq!(elapsed.bound, Duration::from_secs(2));
        // The emitter shows the last message an emitter attached, and the deadline leads.
        let leading = failure
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>())
            .assured("the publish failure carries a message");
        assert_eq!(
            leading,
            "writing a frame to the Syslog server did not finish within timeout_ms 2s"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_client_without_timeout_ms_waits_for_its_server_to_read() {
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        let stream = TcpStream::connect(address)
            .await
            .assured("the listener accepts connections");
        let (mut server, _) = listener.accept().await.assured("the connection arrives");
        let mut connector = SyslogCollectorEmitter {
            config: endpoint_config("tcp", &address.to_string()),
            sender: SyslogSender::Tcp(stream),
            write_timeout: None,
        };
        let frame = vec![b'x'; 4 * 1024 * 1024];
        let reader = nervix_primitives::task::spawn(async move {
            // The server reads nothing for an hour, then all of the frame.
            nervix_primitives::time::sleep(Duration::from_secs(3_600)).await;
            let mut received = Vec::new();
            server
                .read_to_end(&mut received)
                .await
                .assured("the server reads the frame");
            received.len()
        });

        connector
            .publish_payload(&frame)
            .await
            .assured("a write without a deadline waits for the server");
        drop(connector);

        let prefix = format!("{} ", frame.len());
        assert_eq!(
            reader.await.assured("the server task does not panic"),
            prefix.len() + frame.len()
        );
    }

    #[nervix_primitives::test]
    async fn a_frame_written_within_timeout_ms_reaches_its_server() {
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .assured("a loopback TCP port is available");
        let address = listener.local_addr().assured("the listener is bound");
        let stream = TcpStream::connect(address)
            .await
            .assured("the listener accepts connections");
        let (mut server, _) = listener.accept().await.assured("the connection arrives");
        let write_timeout = ConfiguredDeadline::timeout(&[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "30000".to_string(),
        }])
        .assured("thirty seconds is a valid deadline")
        .assured("the entry sets timeout_ms");
        let mut connector = SyslogCollectorEmitter {
            config: endpoint_config("tcp", &address.to_string()),
            sender: SyslogSender::Tcp(stream),
            write_timeout: Some(write_timeout),
        };
        let payload = b"<13>1 - - - - - - within the deadline";

        connector
            .publish_payload(payload)
            .await
            .assured("a server that reads takes the frame within the deadline");
        drop(connector);

        let mut received = Vec::new();
        server
            .read_to_end(&mut received)
            .await
            .assured("the server reads the frame");
        let mut expected = format!("{} ", payload.len()).into_bytes();
        expected.extend_from_slice(payload);
        assert_eq!(received, expected);
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn emitter_rejects_udp_oversize_and_non_transparent_lf() {
        let udp = connector(config("udp", None)).await;
        let datagram_payload = usize::try_from(SYSLOG_FRAME_LIMITS.datagram_payload())
            .assured("a datagram payload fits a 64-bit usize");
        let oversized = vec![
            0_u8;
            datagram_payload
                .checked_add(1)
                .assured("a datagram payload is far below usize::MAX")
        ];
        let maximum = vec![0_u8; datagram_payload];
        assert!(udp.validate_payload(&oversized).is_err());
        assert!(udp.validate_payload(&maximum).is_ok());

        let tcp = connector(config("tcp", Some("non-transparent"))).await;
        assert!(tcp.validate_payload(b"line one\nline two").is_err());
        assert!(tcp.validate_payload(b"one line").is_ok());
    }

    #[test]
    fn octet_count_prefix_has_at_most_ten_digits() {
        assert!(SyslogCollectorEmitter::validate_octet_count_size(9_999_999_999).is_ok());
        assert!(SyslogCollectorEmitter::validate_octet_count_size(10_000_000_000).is_err());
    }
}
