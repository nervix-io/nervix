//! How a RabbitMQ client reaches its broker.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a client's `addr`, CA file and `connect_timeout_ms` into the broker it
//!   connects to, resolving the broker host through the node resolver for every connection, the
//!   budget and order of the address attempts, the TLS handshake against the configured host,
//!   handing the established transport to Lapin for the AMQP handshake, the budget its owner opens
//!   a channel within, and ending a connection that does not open within that budget.
//! - **Depends on.** The connector contract's client configuration helpers, `nervix-dns`,
//!   `error-stack`, Tokio, the URL grammar, Lapin's transport hook and its async runtime, and
//!   `socket2` for the second handle on a connection's socket.
//! - **Must not know.** Queues, consumers, publisher confirms, the host's retry policy, or Models.
//!
//! # One connection
//!
//! Every connection resolves the broker host again through the node resolver, so an expired answer
//! is replaced before anything is dialled. The addresses are tried in resolution order, each within
//! an equal share of what remains of the client's `connect_timeout_ms`, so an address that cannot
//! connect leaves time for the next. An `amqps` connection then completes its TLS handshake within
//! what is left, verifying the broker certificate against the host the address names, whichever of
//! its addresses was dialled. Only then does Lapin start, and it runs the AMQP handshake over that
//! transport within what is still left. The source or sink then opens the channel it reads or
//! publishes through within the rest, so `connect_timeout_ms` bounds every step before the entity
//! is ready, and a connection that runs out of it at any step is reported naming the key and its
//! bound.
//!
//! Lapin's own reconnection stays off, so it asks for a transport exactly once per connection. A
//! connection that is lost is replaced by the caller opening a new one, which resolves again.
//!
//! # A connection that does not open
//!
//! Lapin runs a connection's IO on a thread of its own from the start of the AMQP handshake, and
//! that thread ends only once its socket fails or the broker closes the connection, so a broker
//! that never answers would keep both for ever. Before the transport is handed over, the connection
//! therefore takes a second handle on its TCP socket. Until the connection is open, its handshake
//! done and its owner's channel opened, dropping that handle shuts the socket down, whether the
//! budget ran out, a step failed or the caller stopped waiting. Every read and write of Lapin's loop
//! then fails, the loop and its thread end, and the socket closes. A connection that opened lets
//! the handle go and keeps its socket. Nothing here outlives the connection: the resolver belongs
//! to the node, and a connection that fails before its AMQP handshake has started no Lapin thread.
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

use std::{
    io,
    net::{Shutdown, SocketAddr},
    str::FromStr as _,
};

use async_rs::{Tokio, TokioRuntime, traits::Reactor};
use error_stack::{Context, Report, ResultExt as _};
use lapin::{
    AsyncTcpStream, Connection, ConnectionProperties,
    tcp::TLSConfig,
    uri::{AMQPScheme, AMQPUri},
};
use meticulous::OptionExt as _;
use nervix_connector::{
    ConfiguredDeadline, DeadlineElapsed, client_config_value, client_tls_paths, read_tls_file,
};
use nervix_dns::{DnsLookupFailure, DnsResolver, LookupShare, OperationBudget};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{collections::ConcurrentQueue, time::timeout};
use nervix_recovery::Discarded as _;
use socket2::{SockRef, Socket};
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
    /// The node could not take the second handle on the connection's socket that ends the
    /// connection if it does not open within the budget, such as when it has no file descriptor
    /// left.
    #[error("the socket of the connection to RabbitMQ host '{host}' could not be held")]
    SocketUnavailable { host: String },
    /// The TLS handshake failed or did not finish within the budget, including a broker
    /// certificate that does not name the host.
    #[error("the TLS handshake with RabbitMQ host '{host}' failed")]
    TlsHandshake { host: String },
    #[error("the RabbitMQ client runtime is unavailable")]
    RuntimeUnavailable,
    /// The broker refused or broke off the AMQP handshake, such as for rejected credentials or an
    /// unknown virtual host, or did not finish it within the budget.
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
            | Self::SocketUnavailable { .. }
            | Self::TlsHandshake { .. }
            | Self::RuntimeUnavailable
            | Self::AmqpHandshake { .. } => false,
        }
    }
}

/// Why one round trip that opens a connection's channel did not complete.
#[derive(Debug)]
pub(crate) enum OpeningStepFailure {
    /// The broker or the connection failed the round trip.
    Failed(lapin::Error),
    /// The round trip ran out of what remained of the client's `connect_timeout_ms`.
    Elapsed(DeadlineElapsed),
}

impl OpeningStepFailure {
    /// This failure beneath `context`, its owner's name for the step that failed, with its text
    /// leading the report. A deadline that ran out stays the typed cause beneath `context`, and
    /// Lapin's failure is attached as its text.
    pub(crate) fn beneath<C: Context>(self, context: C) -> Report<C> {
        match self {
            Self::Failed(error) => Report::new(context).attach_printable(error.to_string()),
            Self::Elapsed(elapsed) => {
                let message = elapsed.to_string();
                Report::new(elapsed)
                    .change_context(context)
                    .attach_printable(message)
            }
        }
    }
}

/// What remains of one connection's `connect_timeout_ms` for the steps that open it after its TLS
/// handshake: the AMQP handshake and its owner's channel.
struct OpeningBudget {
    deadline: OperationBudget,
    connect_timeout: ConfiguredDeadline,
}

impl OpeningBudget {
    /// `step`, one round trip that opens the connection, within what remains of the budget.
    /// Running out of it is reported as `operation`, naming `connect_timeout_ms`.
    async fn within<T>(
        &self,
        operation: &'static str,
        step: impl Future<Output = lapin::Result<T>>,
    ) -> Result<T, OpeningStepFailure> {
        match timeout(self.deadline.remaining(), step).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(OpeningStepFailure::Failed(error)),
            Err(_) => Err(OpeningStepFailure::Elapsed(
                self.connect_timeout.elapsed(operation),
            )),
        }
    }
}

/// A connection the broker accepted, whose owner is still opening the channel it reads or
/// publishes through within what remains of the client's `connect_timeout_ms`.
///
/// Dropped before [`Self::opened`], it shuts its socket down, which ends Lapin's IO loop and the
/// thread it runs on, and closes the socket.
pub(crate) struct OpeningConnection {
    connection: Connection,
    socket: OpeningSocket,
    budget: OpeningBudget,
}

impl OpeningConnection {
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    /// `step`, one round trip that opens the channel, within what remains of the connection's
    /// budget. Running out of it is reported as `operation`, naming `connect_timeout_ms`.
    pub(crate) async fn within_budget<T>(
        &self,
        operation: &'static str,
        step: impl Future<Output = lapin::Result<T>>,
    ) -> Result<T, OpeningStepFailure> {
        self.budget.within(operation, step).await
    }

    /// The connection, open: it keeps its socket from now on.
    pub(crate) fn opened(self) -> Connection {
        let Self {
            connection, socket, ..
        } = self;
        socket.release();
        connection
    }
}

/// A second handle on the TCP socket of a connection that is still opening, whose other handle
/// Lapin's IO loop owns.
///
/// Dropping it shuts the socket down: every read and write of that loop fails, so the loop and its
/// thread end and the socket closes. A connection that opened releases it instead.
struct OpeningSocket {
    socket: Option<Socket>,
}

impl OpeningSocket {
    /// A second handle on the socket of `stream`, which the connection to `host` dialled.
    fn hold(stream: &<Tokio as Reactor>::TcpStream, host: &str) -> RabbitMqConnectResult<Self> {
        let socket = SockRef::from(stream.get_ref())
            .try_clone()
            .map_err(|error| {
                Report::new(RabbitMqConnectError::SocketUnavailable {
                    host: host.to_string(),
                })
                .attach_printable(error)
            })?;
        Ok(Self {
            socket: Some(socket),
        })
    }

    /// Lets the socket go without shutting it down, once its connection is open.
    fn release(mut self) {
        drop(self.socket.take());
    }
}

impl Drop for OpeningSocket {
    fn drop(&mut self) {
        let Some(socket) = self.socket.take() else {
            return;
        };
        socket.shutdown(Shutdown::Both).discarded(
            "shutting a socket down fails only once it is no longer connected, and Lapin's loop \
             already sees that connection end",
        );
    }
}

/// The transport a connection dialled, with the second handle on its socket.
struct DialledBroker {
    stream: BrokerStream,
    socket: OpeningSocket,
}

pub(crate) type RabbitMqConnectResult<T> = Result<T, Report<RabbitMqConnectError>>;

/// The broker one RabbitMQ client connects to, and the node resolver its host resolves through.
pub(crate) struct RabbitMqBroker {
    /// The client's `addr`, with the host the URL grammar reads from it.
    uri: AMQPUri,
    transport: BrokerTransport,
    dns: DnsResolver,
    /// How long one connection may take, from resolving the host until its owner's channel is
    /// open.
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

    /// Start a connection to the broker within the client's `connect_timeout_ms`, through its
    /// AMQP handshake; its owner opens its channel within the rest of the budget. The module
    /// documentation describes each step.
    pub(crate) async fn connect(&self) -> RabbitMqConnectResult<OpeningConnection> {
        self.connect_within(self.connect_timeout).await
    }

    async fn connect_within(
        &self,
        connect_timeout: ConfiguredDeadline,
    ) -> RabbitMqConnectResult<OpeningConnection> {
        let deadline = OperationBudget::start(connect_timeout.bound(), LookupShare::Whole);
        let runtime = lapin::runtime::default_runtime().map_err(|error| {
            Report::new(RabbitMqConnectError::RuntimeUnavailable).attach_printable(error)
        })?;
        let addresses = self.resolve(&deadline).await?;
        let DialledBroker { stream, socket } = self
            .dial(&runtime, &addresses, &deadline, connect_timeout)
            .await?;
        let stream = self.secure(stream, &deadline, connect_timeout).await?;
        let budget = OpeningBudget {
            deadline,
            connect_timeout,
        };
        let connection = self.handshake(runtime, stream, &budget).await?;
        Ok(OpeningConnection {
            connection,
            socket,
            budget,
        })
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
    /// an equal share of what remains of the connection budget, with the second handle on its
    /// socket. When no address accepted and the budget ran out, the failure names
    /// `connect_timeout` beneath the unreachable host.
    async fn dial(
        &self,
        runtime: &TokioRuntime,
        addresses: &[SocketAddr],
        deadline: &OperationBudget,
        connect_timeout: ConfiguredDeadline,
    ) -> RabbitMqConnectResult<DialledBroker> {
        let mut failures = Vec::new();
        for attempt in deadline.attempts(addresses) {
            nervix_primitives::task::consume_budget().await;
            let address = attempt.address;
            let share = attempt.budget;
            match timeout(share, runtime.tcp_connect_addr(address)).await {
                Ok(Ok(stream)) => {
                    let socket = OpeningSocket::hold(&stream, self.host())?;
                    return Ok(DialledBroker {
                        stream: AsyncTcpStream::Plain(stream),
                        socket,
                    });
                }
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

    /// Run the AMQP handshake over `stream`, within what remains of the budget.
    ///
    /// Lapin asks its transport hook for a stream once per connection, since its own reconnection
    /// stays off; the hook takes the stream established here from a queue of one. A handshake that
    /// runs out of the budget names `connect_timeout_ms` beneath the failed handshake; the caller
    /// drops the socket's second handle, which ends the loop Lapin started.
    async fn handshake(
        &self,
        runtime: TokioRuntime,
        stream: BrokerStream,
        budget: &OpeningBudget,
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
        );
        budget
            .within("the AMQP handshake with the RabbitMQ broker", connected)
            .await
            .map_err(|failure| {
                failure.beneath(RabbitMqConnectError::AmqpHandshake {
                    host: self.host().to_string(),
                })
            })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
