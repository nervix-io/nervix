//! How a RabbitMQ client reaches its broker.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a client's `addr`, CA file and `connect_timeout_ms` into the broker it
//!   connects to, resolving the broker host through the node resolver for every connection, the
//!   budget and order of the address attempts, the TLS handshake against the configured host, and
//!   handing the established transport to Lapin for the AMQP handshake.
//! - **Depends on.** The connector contract's client configuration helpers, `nervix-dns`,
//!   `error-stack`, Tokio, the URL grammar, and Lapin's transport hook and its async runtime.
//! - **Must not know.** Queues, consumers, publisher confirms, the host's retry policy, or Models.
//!
//! # One connection
//!
//! Every connection resolves the broker host again through the node resolver, so an expired answer
//! is replaced before anything is dialled. The addresses are tried in resolution order, each within
//! an equal share of what remains of the client's `connect_timeout_ms`, so an address that cannot
//! connect leaves time for the next. A connection that runs out of that deadline is reported naming
//! the key and its bound. An `amqps` connection then completes its TLS handshake within what is left,
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

use std::{io, net::SocketAddr, str::FromStr as _};

use async_rs::{Tokio, TokioRuntime, traits::Reactor};
use error_stack::{Report, ResultExt as _};
use lapin::{
    AsyncTcpStream, Connection, ConnectionProperties,
    tcp::TLSConfig,
    uri::{AMQPScheme, AMQPUri},
};
use meticulous::OptionExt as _;
use nervix_connector::{ConfiguredDeadline, client_config_value, client_tls_paths, read_tls_file};
use nervix_dns::{DnsLookupFailure, DnsResolver, LookupShare, OperationBudget};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{collections::ConcurrentQueue, time::timeout};
use thiserror::Error;
use url::{Host, Url};

/// The transport Lapin runs the AMQP protocol over, on the Tokio reactor.
type BrokerStream = AsyncTcpStream<<Tokio as Reactor>::TcpStream>;

/// Why a RabbitMQ client could not open a connection to its broker.
#[derive(Debug, Error)]
pub enum RabbitMqConnectError {
    /// The client's `addr` is missing or is not an AMQP URI.
    #[error("invalid RabbitMQ client address")]
    InvalidAddress,
    /// The client's `connect_timeout_ms` breaks the deadline rule.
    #[error("invalid RabbitMQ client connect_timeout_ms")]
    InvalidConnectTimeout,
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
            Self::InvalidAddress | Self::InvalidConnectTimeout | Self::InvalidCaCertificate => true,
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
    /// How long one connection may take, from resolving the host through the TLS handshake.
    connect_timeout: ConfiguredDeadline,
}

/// How a connection to the broker is carried, as the scheme of its address says.
enum BrokerTransport {
    /// `amqp`: plain TCP.
    Plain,
    /// `amqps`: TLS over TCP, trusting the platform roots and the PEM CA chain of the client's
    /// `tls_ca_file` when it names one.
    Tls { ca_chain: Option<String> },
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
        let connect_timeout = ConfiguredDeadline::connect(entries)
            .change_context(RabbitMqConnectError::InvalidConnectTimeout)?;
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
            connect_timeout,
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

    /// Open a connection to the broker within the client's `connect_timeout_ms`; the module
    /// documentation describes each step.
    pub(crate) async fn connect(&self) -> RabbitMqConnectResult<Connection> {
        self.connect_within(self.connect_timeout).await
    }

    async fn connect_within(
        &self,
        connect_timeout: ConfiguredDeadline,
    ) -> RabbitMqConnectResult<Connection> {
        let deadline = OperationBudget::start(connect_timeout.bound(), LookupShare::Whole);
        let runtime = lapin::runtime::default_runtime().map_err(|error| {
            Report::new(RabbitMqConnectError::RuntimeUnavailable).attach_printable(error)
        })?;
        let addresses = self.resolve(&deadline).await?;
        let stream = self
            .dial(&runtime, &addresses, &deadline, connect_timeout)
            .await?;
        let stream = self.secure(stream, &deadline, connect_timeout).await?;
        self.handshake(runtime, stream).await
    }

    /// Every address the broker host resolves to now, within the connection budget.
    async fn resolve(&self, deadline: &OperationBudget) -> RabbitMqConnectResult<Vec<SocketAddr>> {
        let host = self.host();
        let resolved = self
            .dns
            .resolve(host, self.uri.authority.port, deadline.lookup())
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
    /// an equal share of what remains of the connection budget. When no address accepted and the
    /// budget ran out, the failure names `connect_timeout` beneath the unreachable host.
    async fn dial(
        &self,
        runtime: &TokioRuntime,
        addresses: &[SocketAddr],
        deadline: &OperationBudget,
        connect_timeout: ConfiguredDeadline,
    ) -> RabbitMqConnectResult<BrokerStream> {
        let mut failures = Vec::new();
        for attempt in deadline.attempts(addresses) {
            nervix_primitives::task::consume_budget().await;
            let address = attempt.address;
            let share = attempt.budget;
            match timeout(share, AsyncTcpStream::connect(runtime, address)).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => failures.push(format!("{address}: {error}")),
                Err(_) => failures.push(format!("{address}: no connection within {share:?}")),
            }
        }
        let unreachable = RabbitMqConnectError::Unreachable {
            host: self.host().to_string(),
        };
        let mut report = if deadline.remaining().is_zero() {
            Report::new(connect_timeout.elapsed("connecting to the RabbitMQ broker"))
                .change_context(unreachable)
        } else {
            Report::new(unreachable)
        };
        for failure in failures {
            report = report.attach_printable(failure);
        }
        Err(report)
    }

    /// `stream` itself for `amqp`; for `amqps`, `stream` once its TLS handshake has verified the
    /// broker certificate against the configured host, within what remains of the budget.
    async fn secure(
        &self,
        stream: BrokerStream,
        deadline: &OperationBudget,
        connect_timeout: ConfiguredDeadline,
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
            Err(_) => Err(Report::new(
                connect_timeout.elapsed("the TLS handshake with the RabbitMQ broker"),
            )
            .change_context(RabbitMqConnectError::TlsHandshake {
                host: self.host().to_string(),
            })),
        }
    }

    /// Run the AMQP handshake over `stream`.
    ///
    /// Lapin asks its transport hook for a stream once per connection, since its own reconnection
    /// stays off; the hook takes the stream established here from a queue of one.
    async fn handshake(
        &self,
        runtime: TokioRuntime,
        stream: BrokerStream,
    ) -> RabbitMqConnectResult<Connection> {
        let handed_over = ConcurrentQueue::bounded(1);
        handed_over
            .push(stream)
            .ok()
            .assured("a new queue of one has room for its one transport");
        let connected = Connection::connector(
            self.uri.clone(),
            runtime,
            async move |_uri, _runtime| {
                let handed = handed_over.pop();
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
