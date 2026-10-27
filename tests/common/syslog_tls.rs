//! The TLS sender used by Syslog scenario steps.
//!
//! Layer: test harness.
//! - **Owns.** A prepared client identity, the connection, handshake and framed writes, and
//!   either closing the sender or returning it for a later split-frame write, on one task
//!   independent of the scenario runner's polling.
//! - **Depends on.** Rustls, the binary's runtime and an already prepared client configuration.
//! - **Must not know.** Scenario state, resource directories or the listener's deadline policy.

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_primitives::{net::TcpStream, sync::StdArc, time::Instant};
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use tokio::io::AsyncWriteExt as _;
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::runner_observation::observe_off_the_runner;

/// The client configuration and address captured before a scenario opens its connection.
pub(crate) struct SyslogTlsSender {
    addr: String,
    server_name: ServerName<'static>,
    connector: TlsConnector,
}

impl SyslogTlsSender {
    pub(crate) fn new(
        addr: String,
        server_name: ServerName<'static>,
        config: ClientConfig,
    ) -> Self {
        Self {
            addr,
            server_name,
            connector: TlsConnector::from(StdArc::new(config)),
        }
    }

    /// Start the whole publication now; a delayed caller only learns its result later.
    pub(crate) fn publish(self, frames: Vec<u8>) -> impl Future<Output = ()> + Send {
        observe_off_the_runner(async move {
            let mut stream = self.write_frames(frames).await;
            stream
                .shutdown()
                .await
                .assured("the Syslog TLS test sender closes after publishing its frames");
        })
    }

    /// Start the handshake and initial write together, retaining the sender for a later step.
    pub(crate) fn write(
        self,
        frames: Vec<u8>,
    ) -> impl Future<Output = TlsStream<TcpStream>> + Send {
        observe_off_the_runner(self.write_frames(frames))
    }

    async fn write_frames(self, frames: Vec<u8>) -> TlsStream<TcpStream> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let stream = loop {
            nervix_primitives::task::consume_budget().await;
            match TcpStream::connect(&self.addr).await {
                Ok(stream) => break stream,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out connecting to Syslog listener '{}': {error}",
                        self.addr
                    );
                    nervix_primitives::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        stream
            .set_nodelay(true)
            .assured("the Syslog TLS test connection accepts TCP_NODELAY");
        let mut stream = self
            .connector
            .connect(self.server_name, stream)
            .await
            .assured("the Syslog TLS test session completes its handshake");
        stream
            .write_all(&frames)
            .await
            .assured("the Syslog TLS test sender writes its complete frames");
        stream
            .flush()
            .await
            .assured("the Syslog TLS test sender flushes its complete frames");
        stream
    }
}
