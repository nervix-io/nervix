//! ZeroMQ source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The PULL socket a source configuration declares, whether it binds or connects, the
//!   first frame of each message as the payload, and reopening the socket after a receive failure.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `zeromq`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value, optional_client_config_value,
};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use zeromq::{PullSocket, Socket, SocketRecv, ZmqMessage};

const ZEROMQ: &str = "zeromq";

/// Why a ZeroMQ source could not open or read its socket.
#[derive(Debug, Error)]
pub enum ZeroMqSourceError {
    #[error("invalid ZeroMQ client configuration")]
    ClientConfig,
    #[error("failed to bind ZeroMQ client")]
    Bind,
    #[error("failed to connect ZeroMQ client")]
    Connect,
    #[error("failed to receive a ZeroMQ message")]
    Receive,
}

type ZeroMqSourceResult<T> = Result<T, Report<ZeroMqSourceError>>;

/// The client entries one ZeroMQ source opens its PULL socket from.
#[derive(Clone)]
pub struct ZeroMqSourcePlan {
    config: Vec<ClientConfigEntry>,
}

impl ZeroMqSourcePlan {
    pub fn new(config: Vec<ClientConfigEntry>) -> Self {
        Self { config }
    }

    async fn pull_socket(&self) -> ZeroMqSourceResult<PullSocket> {
        let addr = Self::addr_from_config(&self.config)?;
        let mut socket = PullSocket::new();
        if Self::binds(&self.config) {
            socket.bind(&addr).await.map_err(|source| {
                Report::new(ZeroMqSourceError::Bind).attach_printable(source.to_string())
            })?;
        } else {
            socket.connect(&addr).await.map_err(|source| {
                Report::new(ZeroMqSourceError::Connect).attach_printable(source.to_string())
            })?;
        }
        Ok(socket)
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> ZeroMqSourceResult<String> {
        client_config_value(config, "addr", "ZeroMQ")
            .change_context(ZeroMqSourceError::ClientConfig)
    }

    fn binds(config: &[ClientConfigEntry]) -> bool {
        match optional_client_config_value(config, "bind") {
            Some(value) => value.eq_ignore_ascii_case("true"),
            None => false,
        }
    }
}

/// One received message, whose first frame is the payload. ZeroMQ carries no headers.
pub struct ZeroMqSourceMessage {
    message: ZmqMessage,
    position: (),
    headers: NoIngestHeaders,
}

impl SourceMessage for ZeroMqSourceMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        match self.message.get(0) {
            Some(frame) => frame,
            None => &[],
        }
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

/// One ZeroMQ source instance: its PULL socket, absent until it opens or after a receive failure.
pub struct ZeroMqSource {
    plan: ZeroMqSourcePlan,
    socket: Option<PullSocket>,
}

#[async_trait]
impl SourceConnector for ZeroMqSource {
    type Plan = ZeroMqSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            socket: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.socket.is_none()
    }

    /// Suspension keeps the socket open and stops reading it, so transport flow control and the
    /// peer's high-water marks govern the backlog.
    async fn suspend(&mut self) -> SourceResult<()> {
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.socket.is_none() {
            let socket = self
                .plan
                .pull_socket()
                .await
                .change_context(SourceError::Resume { connector: ZEROMQ })?;
            self.socket = Some(socket);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.socket = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for ZeroMqSource {
    type Message = ZeroMqSourceMessage;
    type Position = ();

    /// Reads until a message with a first frame arrives; a message without frames carries nothing
    /// to ingest and is skipped.
    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        loop {
            tokio::task::consume_budget().await;
            let Some(socket) = self.socket.as_mut() else {
                return Ok(SourceBatch::ResumeRequired);
            };
            let message = match socket.recv().await {
                Ok(message) => message,
                Err(error) => {
                    self.socket = None;
                    return Err(Report::new(ZeroMqSourceError::Receive)
                        .attach_printable(error.to_string())
                        .change_context(SourceError::Read { connector: ZEROMQ }));
                }
            };
            if message.get(0).is_none() {
                continue;
            }
            return Ok(SourceBatch::Messages(vec![ZeroMqSourceMessage {
                message,
                position: (),
                headers: NoIngestHeaders,
            }]));
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn client_config_extractors_handle_defaults_and_missing_keys() {
        let config = [entry("addr", "tcp://127.0.0.1:5555"), entry("bind", "TRUE")];
        assert_eq!(
            ZeroMqSourcePlan::addr_from_config(&config).expect("addr"),
            "tcp://127.0.0.1:5555"
        );
        assert!(ZeroMqSourcePlan::binds(&config));
        assert!(!ZeroMqSourcePlan::binds(&[entry(
            "addr",
            "tcp://127.0.0.1:5555"
        )]));

        let error = ZeroMqSourcePlan::addr_from_config(&[]).expect_err("missing zeromq addr");
        assert!(matches!(
            error.current_context(),
            ZeroMqSourceError::ClientConfig
        ));
        assert!(
            format!("{error:?}").contains("missing ZeroMQ client config key 'addr'"),
            "the cause names the missing key: {error:?}"
        );
    }
}
