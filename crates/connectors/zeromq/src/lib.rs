//! ZeroMQ sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ZeroMQ push-socket configuration, whether the socket binds or connects, and
//!   per-record publication through it.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, and
//!   `zeromq`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use async_trait::async_trait;
use error_stack::Report;
use nervix_connector::{
    PerRecordOutcome, RecordSink, SinkHost, SinkLifecycle, SinkPublishError, SinkRecord,
    SinkStartError, SinkStartResult, client_config_value, optional_client_config_value,
};
use nervix_models::ClientConfigEntry;
use zeromq::{PushSocket, Socket, SocketSend};

const ZEROMQ: &str = "zeromq";

/// What one ZeroMQ sink publishes through: the entries its client configures its socket with.
pub struct ZeroMqSinkConfig {
    pub config: Vec<ClientConfigEntry>,
}

pub struct ZeroMqSink {
    socket: PushSocket,
}

impl ZeroMqSink {
    pub async fn new(config: ZeroMqSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let socket = Self::push_socket_from_config(&config.config).await?;
        Ok(Self { socket })
    }

    async fn push_socket_from_config(config: &[ClientConfigEntry]) -> SinkStartResult<PushSocket> {
        let addr = Self::addr_from_config(config)?;
        let bind = Self::bind_from_config(config);
        let mut socket = PushSocket::new();
        if bind {
            socket.bind(&addr).await.map_err(Self::start_error)?;
        } else {
            socket.connect(&addr).await.map_err(Self::start_error)?;
        }
        Ok(socket)
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> SinkStartResult<String> {
        client_config_value(config, "addr", "ZeroMQ").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: ZEROMQ })
                .attach_printable(message)
        })
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

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: ZEROMQ }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for ZeroMqSink {}

#[async_trait]
impl RecordSink for ZeroMqSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            match self.socket.send(record.payload.into()).await {
                Ok(()) => outcome.deliver(record.position),
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    break;
                }
            }
        }
        outcome
    }
}
