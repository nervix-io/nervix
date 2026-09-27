//! How a RabbitMQ client reaches its broker.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a client's `addr` and CA file into the broker it connects to, resolving the
//!   broker host through the node resolver for every connection, the budget and order of the
//!   address attempts, the TLS handshake against the configured host, and handing the established
//!   transport to Lapin for the AMQP handshake.
//! - **Depends on.** The connector contract's client configuration helpers, `nervix-dns`,
//!   `error-stack`, Tokio, the URL grammar, and Lapin's transport hook and its async runtime.
//! - **Must not know.** Queues, consumers, publisher confirms, the host's retry policy, or Models.
//!
//! # One connection
//!
//! Every connection resolves the broker host again through the node resolver, so an expired answer
//! is replaced before anything is dialled. The addresses are tried in resolution order, each within
//! an equal share of what remains of [`CONNECT_BUDGET`], so an address that cannot connect leaves
//! time for the next. An `amqps` connection then completes its TLS handshake within what is left,
//! verifying the broker certificate against the host the address names, whichever of its addresses
//! was dialled. Only then does Lapin start, and it runs the AMQP handshake over that transport.
//!
//! Lapin's own reconnection stays off, so it asks for a transport exactly once per connection. A
//! connection that is lost is replaced by the caller opening a new one, which resolves again.
//! Nothing here outlives the connection: the resolver belongs to the node, and a connection that
//! fails before the AMQP handshake has started no Lapin thread.
//!
//! # Addresses
//!
//! The broker is the host and port of the client's `addr`, an AMQP URI. Lapin's own URI grammar
//! silently takes `localhost` for an IPv6 literal host, so the host is read with the URL grammar
//! instead: a literal IPv4 or IPv6 address is dialled as written, and a name is resolved.

use std::{io, net::SocketAddr, str::FromStr as _, time::Duration};

use async_rs::{Tokio, TokioRuntime, traits::Reactor};
use error_stack::{Report, ResultExt as _};
use lapin::{
    AsyncTcpStream, Connection, ConnectionProperties,
    tcp::TLSConfig,
    uri::{AMQPScheme, AMQPUri},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{client_config_value, client_tls_paths, read_tls_file};
use nervix_dns::{DnsLookupFailure, DnsResolver};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use tokio::time::{Instant, timeout};
use url::{Host, Url};

/// How long one connection may spend resolving the broker host, reaching one of its addresses and,
/// for `amqps`, completing the TLS handshake. A policy input: generous for a broker across a wide
/// area network, and short enough that a silent name server or an address that drops every packet
/// costs one attempt of the host's retry policy rather than the operating system's connect timeout.
const CONNECT_BUDGET: Duration = Duration::from_secs(30);

/// The transport Lapin runs the AMQP protocol over, on the Tokio reactor.
type BrokerStream = AsyncTcpStream<<Tokio as Reactor>::TcpStream>;

/// Why a RabbitMQ client could not open a connection to its broker.
#[derive(Debug, Error)]
pub enum RabbitMqConnectError {
    /// The client's `addr` is missing or is not an AMQP URI.
    #[error("invalid RabbitMQ client address")]
    InvalidAddress,
    /// The CA file an `amqps` client names could not be read or is not PEM text.
    #[error("invalid RabbitMQ CA certificate")]
    InvalidCaCertificate,
    /// The broker host did not resolve through the node resolver.
    #[error("resolving RabbitMQ host '{host}' failed: {failure}")]
    Resolve {
        host: String,
        failure: DnsLookupFailure,
    },
    /// No address of the broker host accepted a TCP connection within the budget.
    #[error("no address of RabbitMQ host '{host}' accepted a connection")]
    Unreachable { host: String },
    /// The TLS handshake failed or did not finish within the budget, including a broker
    /// certificate that does not name the host.
    #[error("the TLS handshake with RabbitMQ host '{host}' failed")]
    TlsHandshake { host: String },
    #[error("the RabbitMQ client runtime is unavailable")]
    RuntimeUnavailable,
    /// The broker refused or broke off the AMQP handshake, such as for rejected credentials or an
    /// unknown virtual host.
    #[error("the AMQP handshake with RabbitMQ host '{host}' failed")]
    AmqpHandshake { host: String },
}

impl RabbitMqConnectError {
    /// Whether the client's own configuration is at fault, rather than the broker or the path to
    /// it.
    pub fn is_configuration(&self) -> bool {
        match self {
            Self::InvalidAddress | Self::InvalidCaCertificate => true,
            Self::Resolve { .. }
            | Self::Unreachable { .. }
            | Self::TlsHandshake { .. }
            | Self::RuntimeUnavailable
            | Self::AmqpHandshake { .. } => false,
        }
    }
}

pub(crate) type RabbitMqConnectResult<T> = Result<T, Report<RabbitMqConnectError>>;

/// The broker one RabbitMQ client connects to, and the node resolver its host resolves through.
pub(crate) struct RabbitMqBroker {
    /// The client's `addr`, with the host the URL grammar reads from it.
    uri: AMQPUri,
    transport: BrokerTransport,
    dns: DnsResolver,
}

/// How a connection to the broker is carried, as the scheme of its address says.
enum BrokerTransport {
    /// `amqp`: plain TCP.
    Plain,
    /// `amqps`: TLS over TCP, trusting the platform roots and the PEM CA chain of the client's
    /// `tls_ca_file` when it names one.
    Tls { ca_chain: Option<String> },
}

/// What remains of one connection's budget, which resolution, every address attempt and the TLS
/// handshake spend together.
struct ConnectDeadline {
    started: Instant,
    total: Duration,
}

impl RabbitMqBroker {
    /// The broker `entries` name. Reads the CA file an `amqps` client names: blocking I/O on a
    /// small local file, once for each connection.
    pub(crate) fn from_config(
        entries: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> RabbitMqConnectResult<Self> {
        let addr = client_config_value(entries, "addr", "RabbitMQ")
            .change_context(RabbitMqConnectError::InvalidAddress)?;
        let uri = Self::uri(&addr)?;
        let transport = match uri.scheme {
            AMQPScheme::AMQP => BrokerTransport::Plain,
            AMQPScheme::AMQPS => BrokerTransport::Tls {
                ca_chain: Self::ca_chain(entries)?,
            },
        };
        Ok(Self {
            uri,
            transport,
            dns,
        })
    }

    /// `addr` as an AMQP URI whose host is the host the URL grammar reads from it.
    ///
    /// Neither grammar's failure repeats the address, which can carry credentials: an address
    /// without an authority, the one failure Lapin reports by quoting it, is rejected first.
    fn uri(addr: &str) -> RabbitMqConnectResult<AMQPUri> {
        let url = Url::parse(addr).map_err(|error| {
            Report::new(RabbitMqConnectError::InvalidAddress).attach_printable(error)
        })?;
        if url.cannot_be_a_base() {
            return Err(Report::new(RabbitMqConnectError::InvalidAddress)
                .attach_printable("the address names no host"));
        }
        let mut uri = AMQPUri::from_str(addr).map_err(|error| {
            Report::new(RabbitMqConnectError::InvalidAddress).attach_printable(error)
        })?;
        match url.host() {
            Some(Host::Ipv6(address)) => uri.authority.host = address.to_string(),
            Some(Host::Ipv4(address)) => uri.authority.host = address.to_string(),
            Some(Host::Domain(_)) | None => {}
        }
        Ok(uri)
    }

    /// The PEM CA chain of the client's `tls_ca_file`, when it names one.
    fn ca_chain(entries: &[ClientConfigEntry]) -> RabbitMqConnectResult<Option<String>> {
        let tls = client_tls_paths(entries);
        let Some(ca_file) = tls.ca_file.as_ref() else {
            return Ok(None);
        };
        let pem = read_tls_file(ca_file, "TLS CA certificate")
            .change_context(RabbitMqConnectError::InvalidCaCertificate)?;
        let pem = String::from_utf8(pem).map_err(|error| {
            Report::new(RabbitMqConnectError::InvalidCaCertificate).attach_printable(error)
        })?;
        Ok(Some(pem))
    }

    /// The host the client's address names, as it is resolved and as the broker certificate has
    /// to name it.
    fn host(&self) -> &str {
        &self.uri.authority.host
    }

    /// Open a connection to the broker within [`CONNECT_BUDGET`]; the module documentation
    /// describes each step.
    pub(crate) async fn connect(&self) -> RabbitMqConnectResult<Connection> {
        self.connect_within(CONNECT_BUDGET).await
    }

    async fn connect_within(&self, budget: Duration) -> RabbitMqConnectResult<Connection> {
        let deadline = ConnectDeadline::start(budget);
        let runtime = lapin::runtime::default_runtime().map_err(|error| {
            Report::new(RabbitMqConnectError::RuntimeUnavailable).attach_printable(error)
        })?;
        let addresses = self.resolve(&deadline).await?;
        let stream = self.dial(&runtime, &addresses, &deadline).await?;
        let stream = self.secure(stream, &deadline).await?;
        self.handshake(runtime, stream).await
    }

    /// Every address the broker host resolves to now, within the connection budget.
    async fn resolve(&self, deadline: &ConnectDeadline) -> RabbitMqConnectResult<Vec<SocketAddr>> {
        let host = self.host();
        let resolved = self
            .dns
            .resolve(host, self.uri.authority.port, deadline.remaining())
            .await;
        match resolved {
            Ok(addresses) => Ok(addresses),
            Err(report) => {
                let failure = report.current_context().failure();
                Err(report.change_context(RabbitMqConnectError::Resolve {
                    host: host.to_string(),
                    failure,
                }))
            }
        }
    }

    /// A TCP stream to the first of `addresses` that accepts one, trying them in order, each within
    /// an equal share of what remains of the connection budget.
    async fn dial(
        &self,
        runtime: &TokioRuntime,
        addresses: &[SocketAddr],
        deadline: &ConnectDeadline,
    ) -> RabbitMqConnectResult<BrokerStream> {
        let mut report = Report::new(RabbitMqConnectError::Unreachable {
            host: self.host().to_string(),
        });
        for (index, address) in addresses.iter().enumerate() {
            tokio::task::consume_budget().await;
            let untried = addresses
                .len()
                .checked_sub(index)
                .verified("the index enumerates the addresses it is subtracted from");
            let share = deadline.share(untried);
            match timeout(share, AsyncTcpStream::connect(runtime, *address)).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => {
                    report = report.attach_printable(format!("{address}: {error}"));
                }
                Err(_) => {
                    report = report
                        .attach_printable(format!("{address}: no connection within {share:?}"));
                }
            }
        }
        Err(report)
    }

    /// `stream` itself for `amqp`; for `amqps`, `stream` once its TLS handshake has verified the
    /// broker certificate against the configured host, within what remains of the budget.
    async fn secure(
        &self,
        stream: BrokerStream,
        deadline: &ConnectDeadline,
    ) -> RabbitMqConnectResult<BrokerStream> {
        let BrokerTransport::Tls { ca_chain } = &self.transport else {
            return Ok(stream);
        };
        let config = TLSConfig {
            identity: None,
            cert_chain: ca_chain.as_deref(),
        };
        let remaining = deadline.remaining();
        let failed = || {
            Report::new(RabbitMqConnectError::TlsHandshake {
                host: self.host().to_string(),
            })
        };
        match timeout(remaining, stream.into_tls(self.host(), config)).await {
            Ok(Ok(secured)) => Ok(secured),
            Ok(Err(error)) => Err(failed().attach_printable(error)),
            Err(_) => Err(failed().attach_printable(format!("no handshake within {remaining:?}"))),
        }
    }

    /// Run the AMQP handshake over `stream`.
    ///
    /// Lapin asks its transport hook for a stream once per connection, since its own reconnection
    /// stays off; the hook hands over the stream established here through a one-slot channel.
    async fn handshake(
        &self,
        runtime: TokioRuntime,
        stream: BrokerStream,
    ) -> RabbitMqConnectResult<Connection> {
        let (transport, handed_over) = flume::bounded(1);
        transport
            .send(stream)
            .verified("the channel has room for its one transport, and its receiver is held here");
        let connected = Connection::connector(
            self.uri.clone(),
            runtime,
            async move |_uri, _runtime| {
                let handed = handed_over.try_recv();
                match handed {
                    Ok(stream) => Ok(stream),
                    Err(_) => Err(lapin::Error::from(io::Error::other(
                        "a RabbitMQ connection is given its transport once; a lost connection is \
                         replaced by a new one",
                    ))),
                }
            },
            ConnectionProperties::default(),
        )
        .await;
        connected.map_err(|error| {
            Report::new(RabbitMqConnectError::AmqpHandshake {
                host: self.host().to_string(),
            })
            .attach_printable(error)
        })
    }
}

impl ConnectDeadline {
    fn start(total: Duration) -> Self {
        Self {
            started: Instant::now(),
            total,
        }
    }

    /// The time left before the deadline, or zero once it has passed.
    fn remaining(&self) -> Duration {
        match self.total.checked_sub(self.started.elapsed()) {
            Some(remaining) => remaining,
            None => Duration::ZERO,
        }
    }

    /// An equal share of the remaining time for the next of `untried` addresses.
    fn share(&self, untried: usize) -> Duration {
        let untried = u32::try_from(untried)
            .assured("a DNS answer fits in one message and holds far fewer than 2^32 addresses");
        self.remaining()
            .checked_div(untried)
            .verified("the address about to be dialled is itself untried")
    }
}

#[cfg(test)]
mod tests;
