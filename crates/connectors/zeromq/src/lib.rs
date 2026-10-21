//! ZeroMQ source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ZeroMQ push- and pull-socket configuration, whether a socket binds or
//!   connects, a PUSH socket's connection within its client's `connect_timeout_ms` and each send
//!   within its `timeout_ms`, reading the first frame of each received message, and per-record
//!   publication.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, and
//!   `zeromq`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod source;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod sink_tests;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, PerRecordOutcome, RecordSink, SinkFinish, SinkFinishResult, SinkHost,
    SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordId, SinkStartError,
    SinkStartResult, client_config_value, optional_client_config_value,
};
use nervix_models::ClientConfigEntry;
use nervix_primitives::time::{Instant, timeout};
pub use source::{ZeroMqSource, ZeroMqSourceError, ZeroMqSourceMessage, ZeroMqSourcePlan};
use zeromq::{PushSocket, Socket, SocketSend, ZmqMessage};

const ZEROMQ: &str = "zeromq";

/// What one ZeroMQ sink publishes through: the entries its client configures its socket with.
pub struct ZeroMqSinkConfig {
    pub config: Vec<ClientConfigEntry>,
}

pub struct ZeroMqSink {
    socket: PushSocket,
    /// The client's `timeout_ms`, which bounds each send, where it sets one.
    send_timeout: Option<ConfiguredDeadline>,
}

impl ZeroMqSink {
    pub async fn new(config: ZeroMqSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        Self::open(&config.config).await
    }

    /// Opens the PUSH socket `config` declares. The sink reports nothing to its host, so opening it
    /// needs only its client's entries. Both deadline keys are read before the socket opens, so a
    /// value that breaks the deadline rule fails the start whether the socket binds or connects.
    async fn open(config: &[ClientConfigEntry]) -> SinkStartResult<Self> {
        let connect_timeout = Self::connect_timeout(config)?;
        let send_timeout = Self::send_timeout(config)?;
        let socket = Self::push_socket_from_config(config, connect_timeout).await?;
        Ok(Self {
            socket,
            send_timeout,
        })
    }

    async fn push_socket_from_config(
        config: &[ClientConfigEntry],
        connect_timeout: ConfiguredDeadline,
    ) -> SinkStartResult<PushSocket> {
        let addr = Self::addr_from_config(config)?;
        let bind = Self::bind_from_config(config);
        let mut socket = PushSocket::new();
        if bind {
            socket.bind(&addr).await.map_err(Self::start_error)?;
        } else {
            Self::connect(&mut socket, &addr, connect_timeout).await?;
        }
        Ok(socket)
    }

    /// Connects `socket` to its peer at `addr` within the client's `connect_timeout_ms`. The driver
    /// retries a refused connection without limit and waits for the peer's ZMTP handshake without
    /// a deadline, so the deadline is what ends the attempt of a peer that never accepts.
    async fn connect(
        socket: &mut PushSocket,
        addr: &str,
        connect_timeout: ConfiguredDeadline,
    ) -> SinkStartResult<()> {
        match timeout(connect_timeout.bound(), socket.connect(addr)).await {
            Ok(connected) => connected.map_err(Self::start_error),
            Err(_) => Err(Self::connect_elapsed(connect_timeout)),
        }
    }

    /// The deadline the client's `connect_timeout_ms` sets on connecting to the peer, its ZMTP
    /// handshake included.
    fn connect_timeout(config: &[ClientConfigEntry]) -> SinkStartResult<ConfiguredDeadline> {
        ConfiguredDeadline::connect(config)
            .change_context(SinkStartError::InvalidConfiguration { sink: ZEROMQ })
    }

    /// The deadline the client's `timeout_ms` sets on each send, where it sets one.
    fn send_timeout(config: &[ClientConfigEntry]) -> SinkStartResult<Option<ConfiguredDeadline>> {
        ConfiguredDeadline::timeout(config)
            .change_context(SinkStartError::InvalidConfiguration { sink: ZEROMQ })
    }

    /// The failure of a connection that ran out of the client's `connect_timeout_ms`, with the
    /// elapsed deadline beneath it. Its message is attached last, so it leads the report the
    /// emitter shows.
    fn connect_elapsed(deadline: ConfiguredDeadline) -> Report<SinkStartError> {
        let elapsed = deadline.elapsed("connecting to the ZeroMQ peer");
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(SinkStartError::Initialize { sink: ZEROMQ })
            .attach_printable(message)
    }

    /// Sends one message, and fails the send once it has taken longer than the client's
    /// `timeout_ms`: a peer that stops reading fills the connection's buffers and holds the send.
    /// The driver returns a peer to its round robin only when a send to it completes, so this
    /// socket never writes to that connection again and a message a timed-out send left partly
    /// written is never continued; the host opens a new socket after the failed publish.
    async fn send(&mut self, payload: Vec<u8>) -> SinkPublishResult<()> {
        let send_timeout = self.send_timeout;
        let send = self.socket.send(ZmqMessage::from(payload));
        let Some(deadline) = send_timeout else {
            return send.await.map_err(Self::publish_error);
        };
        match timeout(deadline.bound(), send).await {
            Ok(sent) => sent.map_err(Self::publish_error),
            Err(_) => Err(Self::send_elapsed(deadline)),
        }
    }

    /// The failure of a send that ran out of the client's `timeout_ms`, with the elapsed deadline
    /// beneath it. Its message is attached last, so it leads the report the emitter shows.
    fn send_elapsed(deadline: ConfiguredDeadline) -> Report<SinkPublishError> {
        let elapsed = deadline.elapsed("sending a message to the ZeroMQ peer");
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(SinkPublishError::Publish { sink: ZEROMQ })
            .attach_printable(message)
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> SinkStartResult<String> {
        {
            let mapping_input = client_config_value(config, "addr", "ZeroMQ");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(SinkStartError::InvalidConfiguration { sink: ZEROMQ })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn bind_from_config(config: &[ClientConfigEntry]) -> bool {
        match optional_client_config_value(config, "bind") {
            Some(value) => value.eq_ignore_ascii_case("true"),
            None => false,
        }
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: ZEROMQ }).attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external ZeroMQ driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: ZEROMQ }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for ZeroMqSink {
    /// A send that completes has written and flushed its frame to a peer's socket, and what a
    /// timed-out send left belongs to a record its publish failed, so the socket holds no record a
    /// publish reported delivered.
    async fn finish(&mut self, _deadline: Instant) -> SinkFinishResult {
        Ok(SinkFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordSink for ZeroMqSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            match self.send(record.payload).await {
                Ok(()) => outcome.deliver(record.id),
                Err(error) => {
                    outcome.fail(error);
                    break;
                }
            }
        }
        outcome
    }
}
