//! ZeroMq connector entities: ZeroMqPull, ZeroMqPush.
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

impl nervix_connector::EmitterConnector for ZeroMqPushEmitter {
    const KIND: nervix_models::EmitterEntityKind = Self::KIND;
    type Plan = ZeroMqPushEmitterConfig;
    async fn open(plan: Self::Plan, host: EmitterHost) -> EmitterStartResult<Self> {
        Self::prepare(plan, host).await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod emitter_tests;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError,
    EmitterWriteResult, PerRecordOutcome, RecordEmitter, client_config_value,
    optional_bool_client_config_value,
};
use nervix_models::ClientConfigEntry;
use nervix_primitives::time::{Instant, timeout};
use zeromq::{PushSocket, Socket, SocketSend, ZmqMessage};

const ZEROMQ: &str = "zeromq";

/// What one ZeroMQ emitter publishes through: the entries its client configures its socket with.
pub struct ZeroMqPushEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
}

pub struct ZeroMqPushEmitter {
    socket: Option<PushSocket>,
    addr: String,
    bind: bool,
    connect_timeout: ConfiguredDeadline,
    /// The client's `timeout_ms`, which bounds each send, where it sets one.
    send_timeout: Option<ConfiguredDeadline>,
}

impl ZeroMqPushEmitter {
    async fn prepare(
        config: ZeroMqPushEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let connect_timeout = Self::connect_timeout(&config.config)?;
        let send_timeout = Self::send_timeout(&config.config)?;
        let addr = Self::addr_from_config(&config.config)?;
        let bind = Self::bind_from_config(&config.config)?;
        Ok(Self {
            socket: None,
            addr,
            bind,
            connect_timeout,
            send_timeout,
        })
    }

    /// Connects `socket` to its peer at `addr` within the client's `connect_timeout_ms`. The driver
    /// retries a refused connection without limit and waits for the peer's ZMTP handshake without
    /// a deadline, so the deadline is what ends the attempt of a peer that never accepts.
    async fn connect(
        socket: &mut PushSocket,
        addr: &str,
        connect_timeout: ConfiguredDeadline,
    ) -> EmitterStartResult<()> {
        match timeout(connect_timeout.bound(), socket.connect(addr)).await {
            Ok(connected) => connected.map_err(Self::start_error),
            Err(_) => Err(Self::connect_elapsed(connect_timeout)),
        }
    }

    /// The deadline the client's `connect_timeout_ms` sets on connecting to the peer, its ZMTP
    /// handshake included.
    fn connect_timeout(config: &[ClientConfigEntry]) -> EmitterStartResult<ConfiguredDeadline> {
        ConfiguredDeadline::connect(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ZEROMQ })
    }

    /// The deadline the client's `timeout_ms` sets on each send, where it sets one.
    fn send_timeout(
        config: &[ClientConfigEntry],
    ) -> EmitterStartResult<Option<ConfiguredDeadline>> {
        ConfiguredDeadline::timeout(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ZEROMQ })
    }

    /// The failure of a connection that ran out of the client's `connect_timeout_ms`, with the
    /// elapsed deadline beneath it. Its message is attached last, so it leads the report the
    /// emitter shows.
    fn connect_elapsed(deadline: ConfiguredDeadline) -> Report<EmitterOpenError> {
        let elapsed = deadline.elapsed("connecting to the ZeroMQ peer");
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(EmitterOpenError::Initialize { connector: ZEROMQ })
            .attach_printable(message)
    }

    /// Sends one message, and fails the send once it has taken longer than the client's
    /// `timeout_ms`: a peer that stops reading fills the connection's buffers and holds the send.
    /// The driver returns a peer to its round robin only when a send to it completes, so this
    /// socket never writes to that connection again and a message a timed-out send left partly
    /// written is never continued; the retained emitter acquires a new socket before the next publish.
    async fn send(&mut self, payload: Vec<u8>) -> EmitterWriteResult<()> {
        let Some(mut socket) = self.socket.take() else {
            return Err(Report::new(EmitterWriteError::NotInitialized {
                connector: ZEROMQ,
            }));
        };
        let send = socket.send(ZmqMessage::from(payload));
        let sent = match self.send_timeout {
            None => send.await.map_err(Self::publish_error),
            Some(deadline) => match timeout(deadline.bound(), send).await {
                Ok(sent) => sent.map_err(Self::publish_error),
                Err(_) => Err(Self::send_elapsed(deadline)),
            },
        };
        if sent.is_ok() {
            self.socket = Some(socket);
        }
        sent
    }

    /// The failure of a send that ran out of the client's `timeout_ms`, with the elapsed deadline
    /// beneath it. Its message is attached last, so it leads the report the emitter shows.
    fn send_elapsed(deadline: ConfiguredDeadline) -> Report<EmitterWriteError> {
        let elapsed = deadline.elapsed("sending a message to the ZeroMQ peer");
        let message = elapsed.to_string();
        Report::new(elapsed)
            .change_context(EmitterWriteError::Publish { connector: ZEROMQ })
            .attach_printable(message)
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> EmitterStartResult<String> {
        {
            let mapping_input = client_config_value(config, "addr", "ZeroMQ");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(EmitterOpenError::InvalidConfiguration {
                            connector: ZEROMQ,
                        })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn bind_from_config(config: &[ClientConfigEntry]) -> EmitterStartResult<bool> {
        let bind = optional_bool_client_config_value(config, "bind")
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ZEROMQ })?;
        Ok(bind.unwrap_or(false))
    }

    fn start_error(error: zeromq::ZmqError) -> Report<EmitterOpenError> {
        let description = error.to_string();
        Report::new(error)
            .change_context(EmitterOpenError::Initialize { connector: ZEROMQ })
            .attach_printable(description)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external ZeroMQ driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: ZEROMQ })
            .attach_printable(error.to_string())
    }
}

impl ZeroMqPushEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::ZeroMq(nervix_models::ZeroMqEmitterEntity::Push);
}

#[async_trait]
impl EmitterLifecycle for ZeroMqPushEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    fn needs_resume(&mut self) -> bool {
        self.socket.is_none()
    }

    fn resume(
        &mut self,
    ) -> nervix_connector::EmitterLifecycleStep<'_, nervix_connector::EmitterResume> {
        if !self.needs_resume() {
            return nervix_connector::EmitterLifecycleStep::ready(Ok(
                nervix_connector::EmitterResume::Ready,
            ));
        }
        nervix_connector::EmitterLifecycleStep::run(async move {
            if self.socket.is_none() {
                let mut socket = PushSocket::new();
                let opened = if self.bind {
                    match socket.bind(&self.addr).await {
                        Ok(_) => Ok(()),
                        Err(error) => Err(Self::start_error(error)),
                    }
                } else {
                    Self::connect(&mut socket, &self.addr, self.connect_timeout).await
                };
                opened.change_context(nervix_connector::EmitterLifecycleError::Resume {
                    connector: ZEROMQ,
                })?;
                self.socket = Some(socket);
            }
            Ok(nervix_connector::EmitterResume::Ready)
        })
    }

    fn close(&mut self) -> nervix_connector::EmitterLifecycleStep<'_, ()> {
        self.socket = None;
        nervix_connector::EmitterLifecycleStep::ready(Ok(()))
    }

    /// A send that completes has written and flushed its frame to a peer's socket, and what a
    /// timed-out send left belongs to a record its publish failed, so the socket holds no record a
    /// publish reported delivered.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for ZeroMqPushEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
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
