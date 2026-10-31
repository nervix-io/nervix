//! ZeroMQ ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The PULL socket an ingestor configuration declares, whether it binds or connects, the
//!   first frame of each message as the payload, and reopening the socket after a receive failure.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `zeromq`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

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

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorBatch,
    IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, NoIngestHeaders, client_config_value,
    optional_bool_client_config_value,
};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use zeromq::{PullSocket, Socket, SocketRecv, ZmqError, ZmqMessage};

const ZEROMQ: &str = "zeromq";

/// Why a ZeroMQ ingestor could not open or read its socket. Each driver failure stays the ingestor of
/// its variant, so a rendered report carries the cause, such as another socket holding the address.
#[derive(Debug, Error)]
pub enum ZeroMqPullIngestorError {
    #[error("invalid ZeroMQ client configuration")]
    ClientConfig,
    #[error("failed to bind ZeroMQ client to '{addr}'")]
    Bind {
        addr: String,
        #[source]
        source: ZmqError,
    },
    #[error("failed to connect ZeroMQ client to '{addr}'")]
    Connect {
        addr: String,
        #[source]
        source: ZmqError,
    },
    #[error("failed to receive a ZeroMQ message")]
    Receive {
        #[source]
        source: ZmqError,
    },
}

type ZeroMqPullIngestorResult<T> = Result<T, Report<ZeroMqPullIngestorError>>;

/// The client entries one ZeroMQ ingestor opens its PULL socket from.
#[derive(Clone)]
pub struct ZeroMqPullIngestorPlan {
    config: Vec<ClientConfigEntry>,
}

impl ZeroMqPullIngestorPlan {
    pub fn new(config: Vec<ClientConfigEntry>) -> Self {
        Self { config }
    }

    async fn pull_socket(&self) -> ZeroMqPullIngestorResult<PullSocket> {
        let addr = Self::addr_from_config(&self.config)?;
        let bind = Self::binds(&self.config)?;
        let mut socket = PullSocket::new();
        if bind {
            socket.bind(&addr).await.map_err(|source| {
                Report::new(ZeroMqPullIngestorError::Bind {
                    addr: addr.clone(),
                    source,
                })
            })?;
        } else {
            socket.connect(&addr).await.map_err(|source| {
                Report::new(ZeroMqPullIngestorError::Connect {
                    addr: addr.clone(),
                    source,
                })
            })?;
        }
        Ok(socket)
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> ZeroMqPullIngestorResult<String> {
        client_config_value(config, "addr", "ZeroMQ")
            .change_context(ZeroMqPullIngestorError::ClientConfig)
    }

    fn binds(config: &[ClientConfigEntry]) -> ZeroMqPullIngestorResult<bool> {
        let bind = optional_bool_client_config_value(config, "bind")
            .change_context(ZeroMqPullIngestorError::ClientConfig)?;
        Ok(bind.unwrap_or(false))
    }
}

/// One received message, whose first frame is the payload. ZeroMQ carries no headers.
pub struct ZeroMqPullIngestorMessage {
    message: ZmqMessage,
    position: (),
    headers: NoIngestHeaders,
}

impl IngestorMessage for ZeroMqPullIngestorMessage {
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
        IngestMetadataRow::headers(&self.headers)
    }
}

/// One ZeroMQ ingestor instance: its PULL socket, absent until it opens or after a receive failure.
pub struct ZeroMqPullIngestor {
    plan: ZeroMqPullIngestorPlan,
    socket: Option<PullSocket>,
}

#[async_trait]
impl IngestorConnector for ZeroMqPullIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::ZeroMq(nervix_models::ZeroMqIngestorEntity::Pull);

    type Plan = ZeroMqPullIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
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
    async fn suspend(&mut self) -> IngestorResult<()> {
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.socket.is_none() {
            let socket = self
                .plan
                .pull_socket()
                .await
                .change_context(IngestorError::Resume { connector: ZEROMQ })?;
            self.socket = Some(socket);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.socket = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for ZeroMqPullIngestor {
    type Message = ZeroMqPullIngestorMessage;
    type Position = ();

    /// Reads until a message with a first frame arrives; a message without frames carries nothing
    /// to ingest and is skipped.
    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(socket) = self.socket.as_mut() else {
                return Ok(IngestorBatch::ResumeRequired);
            };
            let Some(received) = cancellation.unless_cancelled(socket.recv()).await else {
                return Ok(IngestorBatch::Interrupted {
                    gathered: Vec::new(),
                });
            };
            let message = match received {
                Ok(message) => message,
                Err(source) => {
                    self.socket = None;
                    return Err(Report::new(ZeroMqPullIngestorError::Receive { source })
                        .change_context(IngestorError::Read { connector: ZEROMQ }));
                }
            };
            if message.get(0).is_none() {
                continue;
            }
            return Ok(IngestorBatch::Messages(vec![ZeroMqPullIngestorMessage {
                message,
                position: (),
                headers: NoIngestHeaders,
            }]));
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use meticulous::ResultExt as _;
    use nervix_primitives::net::TcpListener;

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
            ZeroMqPullIngestorPlan::addr_from_config(&config).expect("addr"),
            "tcp://127.0.0.1:5555"
        );
        assert!(ZeroMqPullIngestorPlan::binds(&config).assured("TRUE is a valid Boolean"));
        assert!(
            !ZeroMqPullIngestorPlan::binds(&[entry("addr", "tcp://127.0.0.1:5555")])
                .assured("omitting bind selects connect")
        );

        let error = ZeroMqPullIngestorPlan::addr_from_config(&[]).expect_err("missing zeromq addr");
        assert!(matches!(
            error.current_context(),
            ZeroMqPullIngestorError::ClientConfig
        ));
        assert!(
            format!("{error:?}").contains("missing ZeroMQ client config key 'addr'"),
            "the cause names the missing key: {error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn invalid_bind_is_a_configuration_refusal_before_the_socket_opens() {
        let plan = ZeroMqPullIngestorPlan::new(vec![
            entry("addr", "tcp://127.0.0.1:not-a-port"),
            entry("bind", "yes"),
        ]);
        let error = match plan.pull_socket().await {
            Ok(_) => panic!("an invalid bind value cannot open a socket"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            ZeroMqPullIngestorError::ClientConfig
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());
    }

    #[nervix_primitives::test]
    async fn a_bind_another_listener_refuses_names_the_address_and_keeps_the_cause() {
        let Ok(holder) = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await else {
            panic!("loopback has no free ephemeral port");
        };
        let Ok(taken) = holder.local_addr() else {
            panic!("a bound listener has no address");
        };
        // The refusal any second listener on the address meets, which the report must carry.
        let refusal = match TcpListener::bind(taken).await {
            Ok(_) => panic!("a second listener bound {taken} while the first listens on it"),
            Err(refusal) => refusal,
        };
        let addr = format!("tcp://{taken}");
        let plan = ZeroMqPullIngestorPlan::new(vec![entry("addr", &addr), entry("bind", "true")]);

        let error = match plan.pull_socket().await {
            Ok(_) => panic!("the PULL socket bound {addr} while another socket listens on it"),
            Err(error) => error,
        };

        let ZeroMqPullIngestorError::Bind { addr: refused, .. } = error.current_context() else {
            panic!("a refused bind reports a bind failure: {error:?}");
        };
        assert_eq!(refused, &addr);
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains(&refusal.to_string()),
            "the rendered chain carries the refusal '{refusal}': {rendered}"
        );
        drop(holder);
    }

    #[nervix_primitives::test]
    async fn a_connect_the_driver_refuses_names_the_address_and_keeps_the_cause() {
        // The driver refuses an endpoint whose port it cannot read before it dials anything.
        let addr = "tcp://127.0.0.1:not-a-port".to_string();
        let plan = ZeroMqPullIngestorPlan::new(vec![entry("addr", &addr)]);

        let error = match plan.pull_socket().await {
            Ok(_) => panic!("the PULL socket connected to {addr}"),
            Err(error) => error,
        };

        let ZeroMqPullIngestorError::Connect {
            addr: refused,
            source,
        } = error.current_context()
        else {
            panic!("a refused connect reports a connect failure: {error:?}");
        };
        assert_eq!(refused, &addr);
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains(&source.to_string()),
            "the rendered chain carries the driver's cause '{source}': {rendered}"
        );
    }
}
