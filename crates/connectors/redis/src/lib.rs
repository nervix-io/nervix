//! Redis sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Redis client a configuration declares, the node's shared pool of command
//!   connections, and publication of each record to a channel.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, `redis` and
//!   `bb8`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation. The pool it borrows from is leased by the host, which owns the
//!   interest that keeps it open and the wait it records while a connection is handed over.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use async_trait::async_trait;
use error_stack::Report;
use nervix_connector::{
    PerRecordOutcome, RecordSink, ServiceUrl, SinkHost, SinkLifecycle, SinkPublishError,
    SinkRecord, SinkStartResult, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_models::{ChannelName, ClientConfigEntry, ClientPoolBounds};
use redis::{
    AsyncCommands, Client as RedisClient, ClientTlsConfig, ErrorKind as RedisErrorKind,
    ServerErrorKind, TlsCertificates as RedisTlsCertificates,
};
use thiserror::Error;
use triomphe::Arc;

const REDIS: &str = "redis";

/// The node's shared pool of Redis command connections for one named client.
///
/// Every entry is an independent connection, so the declared maximum bounds sockets rather than
/// handles, and a checked-out connection carries one publisher's outstanding operation alone.
pub type RedisCommandPool = bb8::Pool<RedisClient>;

/// One command connection borrowed from that pool, returned when it is dropped.
pub type RedisPooledConnection<'pool> = bb8::PooledConnection<'pool, RedisClient>;

/// Why one Redis client or its command pool could not be opened.
#[derive(Debug, Error)]
pub enum RedisClientError {
    #[error("missing Redis client config key '{key}'")]
    MissingConfig { key: &'static str },
    #[error("invalid Redis client configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("failed to connect to Redis: {reason}")]
    Connect { reason: String },
}

/// Open the node's shared Redis command pool for one named client, sized by its declared bounds.
///
/// Subscription connections are not drawn from here: a Redis Pub/Sub ingestor owns its own
/// dedicated connection, so command-pool pressure never interrupts a healthy subscription.
pub async fn open_redis_command_pool(
    config: &[ClientConfigEntry],
    bounds: ClientPoolBounds,
) -> Result<RedisCommandPool, Report<RedisClientError>> {
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(Report::new(RedisClientError::MissingConfig { key: "addr" }));
    };
    let client = redis_client(addr, config)?;
    bb8::Pool::builder()
        .max_size(bounds.maximum().get())
        .min_idle(Some(bounds.minimum()))
        .build(client)
        .await
        .map_err(|source| {
            Report::new(RedisClientError::Connect {
                reason: source.to_string(),
            })
        })
}

/// The Redis client one named client's configuration declares, with the TLS material a
/// `rediss://` address names.
///
/// The command pool draws its connections from it, and a Pub/Sub source opens its dedicated
/// subscription connection from the same client.
pub fn redis_client(
    addr: &str,
    config: &[ClientConfigEntry],
) -> Result<RedisClient, Report<RedisClientError>> {
    let tls = client_tls_paths(config);
    let secure = ServiceUrl::new(addr, "Redis addr")
        .has_scheme("rediss")
        .map_err(|error| invalid_client_config(error.current_context()))?;
    if secure && (tls.ca_file.is_some() || tls.cert_file.is_some() || tls.key_file.is_some()) {
        RedisClient::build_with_tls(
            addr,
            RedisTlsCertificates {
                client_tls: match (&tls.cert_file, &tls.key_file) {
                    (Some(cert_file), Some(key_file)) => Some(ClientTlsConfig {
                        client_cert: read_tls_file(cert_file, "TLS certificate")
                            .map_err(|error| invalid_client_config(error.current_context()))?,
                        client_key: read_tls_file(key_file, "TLS private key")
                            .map_err(|error| invalid_client_config(error.current_context()))?,
                    }),
                    (None, None) => None,
                    _ => {
                        return Err(invalid_client_config(
                            "Redis TLS client authentication requires both 'tls_cert_file' and \
                             'tls_key_file'",
                        ));
                    }
                },
                root_cert: match tls.ca_file.as_ref() {
                    Some(ca_file) => Some(
                        read_tls_file(ca_file, "TLS CA certificate")
                            .map_err(|error| invalid_client_config(error.current_context()))?,
                    ),
                    None => None,
                },
            },
        )
        .map_err(invalid_client_config)
    } else {
        RedisClient::open(addr).map_err(invalid_client_config)
    }
}

fn invalid_client_config(reason: impl std::fmt::Display) -> Report<RedisClientError> {
    Report::new(RedisClientError::InvalidConfig {
        reason: reason.to_string(),
    })
}

/// The pool one Redis sink borrows command connections from, as the host leases it.
///
/// The host owns the interest that keeps the pool open and the wait it records while a connection
/// is handed over; this handle holds both for as long as the sink publishes through them.
pub trait RedisPoolServices: Send + Sync + 'static {
    /// The shared command pool this sink's client opened on this node.
    fn pool(&self) -> RedisCommandPool;

    /// Records this sink's wait for a free connection until the returned guard is dropped.
    fn begin_pool_wait(&self) -> RedisPoolWait;
}

/// An opaque record of one sink's wait for a pooled connection, ended when it is dropped.
pub struct RedisPoolWait {
    /// Held until the pool hands this sink a connection; dropping it ends the recorded wait.
    _wait: Box<dyn Send>,
}

impl RedisPoolWait {
    pub fn new(wait: impl Send + 'static) -> Self {
        Self {
            _wait: Box::new(wait),
        }
    }
}

struct RedisPoolInner {
    services: Box<dyn RedisPoolServices>,
}

/// A handle to the host-leased pool, shared by the sink and whatever else the host hands it to.
#[derive(Clone)]
pub struct RedisPoolHandle {
    inner: Arc<RedisPoolInner>,
}

impl RedisPoolHandle {
    pub fn new(services: impl RedisPoolServices) -> Self {
        Self {
            inner: Arc::new(RedisPoolInner {
                services: Box::new(services),
            }),
        }
    }

    fn pool(&self) -> RedisCommandPool {
        self.inner.services.pool()
    }

    fn begin_pool_wait(&self) -> RedisPoolWait {
        self.inner.services.begin_pool_wait()
    }
}

/// What one Redis sink publishes to: the pool it borrows from and the channel it writes.
pub struct RedisSinkConfig {
    pub pool: RedisPoolHandle,
    pub channel: ChannelName,
}

pub struct RedisSink {
    pool: RedisPoolHandle,
    channel: ChannelName,
}

impl RedisSink {
    pub fn new(config: RedisSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        Ok(Self {
            pool: config.pool,
            channel: config.channel,
        })
    }

    /// Borrow a command connection for one publish, reporting the wait until the pool hands one
    /// over. The connection returns to the pool as soon as the publish completes.
    async fn connection<'pool>(
        pool: &'pool RedisCommandPool,
        handle: &RedisPoolHandle,
    ) -> Result<RedisPooledConnection<'pool>, Report<SinkPublishError>> {
        let waiting = handle.begin_pool_wait();
        let connection = pool.get().await.map_err(Self::publish_error);
        drop(waiting);
        connection
    }

    fn is_record_failure(error: &redis::RedisError) -> bool {
        if !matches!(
            error.kind(),
            RedisErrorKind::Server(ServerErrorKind::ResponseError)
        ) {
            return false;
        }
        let detail = error.detail().unwrap_or_default().to_ascii_lowercase();
        detail.contains("protocol error: invalid bulk length")
            || detail.contains("string exceeds maximum allowed size")
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: REDIS }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for RedisSink {}

#[async_trait]
impl RecordSink for RedisSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let pool = self.pool.pool();
        for record in records {
            tokio::task::consume_budget().await;
            // Borrowed per publish and returned with the guard: an emitter between publishes, or
            // waiting out a flush interval, holds no connection at all.
            let mut connection = match Self::connection(&pool, &self.pool).await {
                Ok(connection) => connection,
                Err(error) => {
                    outcome.fail(error);
                    break;
                }
            };
            let published: redis::RedisResult<i64> = connection
                .publish(self.channel.as_str(), record.payload.as_slice())
                .await;
            match published {
                Ok(_) => outcome.deliver(record.position),
                Err(error) if Self::is_record_failure(&error) => {
                    outcome
                        .reject(record.rejected(format!("Redis rejected emitted record: {error}")));
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    break;
                }
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_definitive_redis_command_rejections_are_record_failures() {
        let rejected = redis::RedisError::from((
            RedisErrorKind::Server(ServerErrorKind::ResponseError),
            "record rejected",
            "Protocol error: invalid bulk length".to_string(),
        ));
        let ambiguous = redis::RedisError::from((
            RedisErrorKind::Server(ServerErrorKind::ResponseError),
            "record rejected",
            "OOM command not allowed when used memory is greater than maxmemory".to_string(),
        ));
        let disconnected = redis::RedisError::from((RedisErrorKind::Io, "connection interrupted"));

        assert!(RedisSink::is_record_failure(&rejected));
        assert!(!RedisSink::is_record_failure(&ambiguous));
        assert!(!RedisSink::is_record_failure(&disconnected));
    }
}
