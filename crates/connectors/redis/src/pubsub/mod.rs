//! Redis ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Redis client a configuration declares, the `connect_timeout_ms` that bounds
//!   opening a connection and checking one out, the node's shared pool of command connections, the
//!   `POOL SIZE MIN` connections it keeps established, checked-out ones counted with idle ones, and
//!   the idle and maximum lifetimes it retires them at, the dedicated subscription connection a
//!   Pub/Sub ingestor reads a channel through, and publication of each record to a channel.
//! - **Depends on.** The connector contract, node DNS resolver, vocabulary values, `error-stack`,
//!   Tokio, `redis`, `bb8` and `futures-util`'s joins.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation. The pool it borrows from is leased by the host, which owns the
//!   interest that keeps it open and the wait it records while a connection is handed over.

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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod counting_redis;
mod ingestor;
mod pool_minimum;
#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod test_fixture;

use std::{io, net::SocketAddr, num::NonZeroU32, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
pub use ingestor::{
    RedisPubSubIngestor, RedisPubSubIngestorError, RedisPubSubIngestorMessage,
    RedisPubSubIngestorPlan,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError, PerRecordOutcome,
    RecordEmitter, ServiceUrl, client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::{
    DnsLookupError, DnsLookupFailure, DnsLookupReport, DnsResolver, HOOK_LOOKUP_BUDGET,
};
use nervix_models::{
    ClientConfigEntry, ClientConfigSchema, ClientDeadline, ClientPoolBounds, ConnectorKind,
    RedisChannelName,
};
use nervix_primitives::{sync::Arc, time::Instant};
use pool_minimum::{
    ConnectionClosures, EstablishedConnections, KeptPool, PoolMinimum, PoolMinimumError,
    Restoration,
};
use redis::{
    AsyncCommands, AsyncConnectionConfig, Client as RedisClient, ClientTlsConfig,
    ErrorKind as RedisErrorKind, RedisFuture, ServerErrorKind,
    TlsCertificates as RedisTlsCertificates,
    aio::{ConnectionLike, MultiplexedConnection},
    io::AsyncDNSResolver,
};
use thiserror::Error;

const REDIS: &str = "redis";

/// Redis's DNS hook, backed by the resolver loaded for this node.
#[derive(Clone)]
struct RedisDns(DnsResolver);

impl AsyncDNSResolver for RedisDns {
    fn resolve<'a, 'b: 'a>(
        &'a self,
        host: &'b str,
        port: u16,
    ) -> RedisFuture<'a, Box<dyn Iterator<Item = SocketAddr> + Send + 'a>> {
        Box::pin(async move {
            let addresses = self.0.resolve(host, port, HOOK_LOOKUP_BUDGET).await;
            match addresses {
                Ok(addresses) => {
                    let iter: Box<dyn Iterator<Item = SocketAddr> + Send + 'a> =
                        Box::new(addresses.into_iter());
                    Ok(iter)
                }
                Err(error) => Err(io::Error::other(DnsLookupReport::from(error)).into()),
            }
        })
    }
}

/// bb8 opens each physical command connection with Redis's configured DNS hook, within the
/// client's `connect_timeout_ms`: name resolution, the connection, TLS and Redis's own setup.
pub struct RedisCommandManager {
    client: RedisClient,
    dns: DnsResolver,
    connect_timeout: ConfiguredDeadline,
    /// Handed to every connection this manager opens, which signals it when it closes.
    closures: ConnectionClosures,
}

impl bb8::ManageConnection for RedisCommandManager {
    type Connection = RedisCommandConnection;
    type Error = redis::RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.connect_timeout.bound()))
            .set_dns_resolver(RedisDns(self.dns.clone()));
        let connection = self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        Ok(RedisCommandConnection {
            connection,
            closures: self.closures.clone(),
        })
    }

    async fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::Cmd::ping().query_async(connection).await?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err((RedisErrorKind::UnexpectedReturnType, "ping request", pong).into())
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// One physical command connection of the pool, which tells the keeper of the pool's minimum when
/// the pool closes it.
#[derive(Debug)]
pub struct RedisCommandConnection {
    connection: MultiplexedConnection,
    closures: ConnectionClosures,
}

impl ConnectionLike for RedisCommandConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, redis::Value> {
        self.connection.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<redis::Value>> {
        self.connection.req_packed_commands(pipeline, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}

impl Drop for RedisCommandConnection {
    fn drop(&mut self) {
        self.closures.closed();
    }
}

/// The node's shared pool of Redis command connections for one named client.
///
/// Every entry is an independent connection, so the declared maximum bounds sockets rather than
/// handles, and a checked-out connection carries one publisher's outstanding operation alone.
/// Checking a connection out waits at most the client's `connect_timeout_ms`, which also bounds
/// opening each new connection. The pool keeps the declared minimum of established connections,
/// counting the ones publishers have checked out with the idle ones, and opens another only for a
/// publisher that finds none idle. It closes a connection idle for the client's
/// `pool_idle_timeout_ms` or older than its `pool_max_lifetime_ms`, then opens connections again to
/// keep the minimum.
#[derive(Clone, Debug)]
pub struct RedisCommandPool {
    shared: Arc<RedisCommandPoolShared>,
}

/// What every handle to one command pool shares.
#[derive(Debug)]
struct RedisCommandPoolShared {
    commands: CommandConnections,
    /// The task keeping `POOL SIZE MIN` established, stopped with the last handle. A pool whose
    /// minimum is zero keeps nothing.
    _minimum: Option<PoolMinimum>,
}

/// The pool's connections, and the deadline that every checkout and every new connection keeps.
///
/// The task keeping the pool's minimum holds its own copy, so it never holds the handles that stop
/// it.
#[derive(Clone, Debug)]
struct CommandConnections {
    connections: bb8::Pool<RedisCommandManager>,
    connect_timeout: ConfiguredDeadline,
}

impl CommandConnections {
    /// Open `minimum` connections into the pool before anything borrows from it.
    ///
    /// Nothing else holds the pool yet, so each one fits its maximum, and a connection that fails
    /// keeps its driver's typed cause, which the open reports.
    async fn open_minimum(&self, minimum: NonZeroU32) -> Result<(), Report<RedisClientError>> {
        let opening = (0..minimum.get()).map(|_| self.connections.dedicated_connection());
        let opened = futures_util::future::try_join_all(opening).await;
        let opened = match opened {
            Ok(opened) => opened,
            Err(source) => return Err(redis_pool_error(source, self.connect_timeout)),
        };
        for connection in opened {
            self.connections.add(connection).assured(
                "the minimum fits the maximum, nothing else borrows from the pool yet, and a \
                 command connection never reports itself broken",
            );
        }
        Ok(())
    }

    /// Why one checkout did not hand a connection over, beneath the restoration it belonged to.
    fn restoration_error(
        &self,
        error: bb8::RunError<redis::RedisError>,
        restoration: Restoration,
    ) -> Report<PoolMinimumError> {
        let not_restored = PoolMinimumError::NotRestored {
            minimum: restoration.minimum,
        };
        match error {
            bb8::RunError::User(source) => {
                redis_pool_error(source, self.connect_timeout).change_context(not_restored)
            }
            bb8::RunError::TimedOut => {
                let elapsed = self
                    .connect_timeout
                    .elapsed("checking out a Redis command connection");
                Report::new(elapsed).change_context(not_restored)
            }
        }
    }
}

impl KeptPool for CommandConnections {
    fn established(&self) -> EstablishedConnections {
        let state = self.connections.state();
        EstablishedConnections {
            total: state.connections,
            idle: state.idle_connections,
        }
    }

    async fn restore(&self, restoration: Restoration) -> Result<(), Report<PoolMinimumError>> {
        let checkouts = (0..restoration.checkouts.get()).map(|_| self.connections.get());
        // Every checkout is held until all of them finish, so the idle connections are taken once
        // and the pool opens one for each checkout left without one.
        let held = futures_util::future::join_all(checkouts).await;
        let mut failure = None;
        for checkout in held {
            if let Err(error) = checkout
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        match failure {
            None => Ok(()),
            Some(error) => Err(self.restoration_error(error, restoration)),
        }
    }
}

/// One command connection borrowed from that pool, returned when it is dropped.
pub type RedisPooledConnection<'pool> = bb8::PooledConnection<'pool, RedisCommandManager>;

/// Why one Redis client or its command pool could not be opened.
#[derive(Debug, Error)]
pub enum RedisClientError {
    #[error("missing Redis client config key '{key}'")]
    MissingConfig { key: &'static str },
    /// The client's `connect_timeout_ms` breaks the deadline rule, which the cause beneath names.
    #[error("invalid Redis client connect_timeout_ms")]
    InvalidConnectTimeout,
    /// One of the command pool's lifetimes breaks its declared value rule, which the cause beneath
    /// names.
    #[error("invalid Redis client {key}")]
    InvalidPoolLifetime { key: &'static str },
    #[error("invalid Redis client configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("failed to connect to Redis: {reason}")]
    Connect { reason: String },
    #[error("resolving Redis host '{host}' failed: {failure}")]
    Resolve {
        host: String,
        failure: DnsLookupFailure,
    },
}

/// Open the node's shared Redis command pool for one named client, sized by its declared bounds and
/// retiring connections at its declared lifetimes.
///
/// Subscription connections are not drawn from here: a Redis Pub/Sub ingestor owns its own
/// dedicated connection, so command-pool pressure never interrupts a healthy subscription.
pub async fn open_redis_command_pool(
    config: &[ClientConfigEntry],
    bounds: ClientPoolBounds,
    dns: DnsResolver,
) -> Result<RedisCommandPool, Report<RedisClientError>> {
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(Report::new(RedisClientError::MissingConfig { key: "addr" }));
    };
    let client = redis_client(addr, config)?;
    let connect_timeout = ConfiguredDeadline::connect(config)
        .change_context(RedisClientError::InvalidConnectTimeout)?;
    let lifetimes = RedisPoolLifetimes::read(config)?;
    let closures = ConnectionClosures::default();
    let manager = RedisCommandManager {
        client,
        dns,
        connect_timeout,
        closures: closures.clone(),
    };
    // Without the driver's idle minimum, which counts idle connections only and would open another
    // beside every connection a publisher checks out: the pool's own keeper holds the minimum.
    let connections = bb8::Pool::builder()
        .max_size(bounds.maximum().get())
        .connection_timeout(connect_timeout.bound())
        .idle_timeout(Some(lifetimes.idle))
        .max_lifetime(Some(lifetimes.maximum))
        .build_unchecked(manager);
    let commands = CommandConnections {
        connections,
        connect_timeout,
    };
    let minimum = match NonZeroU32::new(bounds.minimum()) {
        None => None,
        Some(minimum) => {
            commands.open_minimum(minimum).await?;
            Some(PoolMinimum::keep(commands.clone(), minimum, closures))
        }
    };
    Ok(RedisCommandPool {
        shared: Arc::new(RedisCommandPoolShared {
            commands,
            _minimum: minimum,
        }),
    })
}

/// How long the command pool keeps a connection, as the client's effective configuration declares
/// it: an omitted key takes the default the client schema declares for it.
///
/// `bb8` closes a connection idle for `idle`, the ones it keeps for the declared minimum included,
/// and one that has lived `maximum` when it is returned or while it is idle; it checks its idle
/// connections every 30 seconds, its own reaper interval.
#[derive(Debug, Clone, Copy)]
struct RedisPoolLifetimes {
    /// `pool_idle_timeout_ms`: how long a connection may sit idle in the pool.
    idle: Duration,
    /// `pool_max_lifetime_ms`: how old a connection may become.
    maximum: Duration,
}

impl RedisPoolLifetimes {
    fn read(config: &[ClientConfigEntry]) -> Result<Self, Report<RedisClientError>> {
        let schema = ConnectorKind::Redis
            .client()
            .assured("the Redis connector declares a configured client")
            .config;
        let effective = schema.effective_entries(config);
        Ok(Self {
            idle: Self::lifetime(schema, &effective, "pool_idle_timeout_ms")?,
            maximum: Self::lifetime(schema, &effective, "pool_max_lifetime_ms")?,
        })
    }

    /// The lifetime the declared key `name` sets in `effective`, held to its declared value rule.
    fn lifetime(
        schema: ClientConfigSchema,
        effective: &[ClientConfigEntry],
        name: &'static str,
    ) -> Result<Duration, Report<RedisClientError>> {
        let key = schema
            .key(name)
            .assured("the Redis client schema declares both command pool lifetimes");
        let value = optional_client_config_value(effective, name)
            .assured("the effective configuration supplies every defaulted key");
        key.validate(value)
            .change_context(RedisClientError::InvalidPoolLifetime { key: name })?;
        let lifetime = ClientDeadline::parse(value)
            .verified("the declared value rule checked above parses the same milliseconds");
        Ok(lifetime.duration())
    }
}

/// Why one command connection could not be opened: the name its address resolves, or the
/// connection itself, which names `connect_timeout` beneath it when the driver ran out of it.
fn redis_pool_error(
    source: redis::RedisError,
    connect_timeout: ConfiguredDeadline,
) -> Report<RedisClientError> {
    if let Some(lookup) = DnsLookupError::find_in(&source) {
        return Report::new(RedisClientError::Resolve {
            host: lookup.name().to_string(),
            failure: lookup.failure(),
        })
        .attach_printable(source);
    }
    let connect = RedisClientError::Connect {
        reason: source.to_string(),
    };
    if source.is_timeout() {
        let elapsed = connect_timeout.elapsed("opening a Redis command connection");
        return Report::new(elapsed)
            .change_context(connect)
            .attach_printable(elapsed.to_string());
    }
    Report::new(connect)
}

/// The Redis client one named client's configuration declares, with the TLS material a
/// `rediss://` address names.
///
/// The command pool draws its connections from it; a Pub/Sub ingestor uses its connection settings
/// after opening a dedicated stream.
fn redis_client(
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

/// The pool one Redis emitter borrows command connections from, as the host leases it.
///
/// The host owns the interest that keeps the pool open and the wait it records while a connection
/// is handed over; this handle holds both for as long as the emitter publishes through them.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained host service is invoked by each admitted driver operation"
    )
)]
pub trait RedisPoolServices: Send + Sync + 'static {
    /// The shared command pool this emitter's client opened on this node.
    fn pool(&self) -> RedisCommandPool;

    /// Records this emitter's wait for a free connection until the returned guard is dropped.
    fn begin_pool_wait(&self) -> RedisPoolWait;
}

/// An opaque record of one emitter's wait for a pooled connection, ended when it is dropped.
pub struct RedisPoolWait {
    /// Held until the pool hands this emitter a connection; dropping it ends the recorded wait.
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

/// A handle to the host-leased pool, shared by the emitter and whatever else the host hands it to.
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

/// What one Redis emitter publishes to: the pool it borrows from and the channel it writes.
pub struct RedisPubSubEmitterConfig {
    pub pool: RedisPoolHandle,
    pub channel: RedisChannelName,
}

pub struct RedisPubSubEmitter {
    pool: RedisPoolHandle,
    channel: RedisChannelName,
}

impl RedisPubSubEmitter {
    pub fn new(config: RedisPubSubEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        Ok(Self {
            pool: config.pool,
            channel: config.channel,
        })
    }

    /// Borrow a command connection for one publish, reporting the wait until the pool hands one
    /// over. The connection returns to the pool as soon as the publish completes, and a pool that
    /// hands none over within the client's `connect_timeout_ms` fails the publish naming it.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the bb8 driver owns polling the pool future")
    )]
    async fn connection<'pool>(
        pool: &'pool RedisCommandPool,
        handle: &RedisPoolHandle,
    ) -> Result<RedisPooledConnection<'pool>, Report<EmitterWriteError>> {
        let commands = &pool.shared.commands;
        let mut borrow = std::pin::pin!(commands.connections.get());
        let mut waiting = None;
        let connection = std::future::poll_fn(|context| {
            let result = borrow.as_mut().poll(context);
            if result.is_pending() && waiting.is_none() {
                waiting = Some(handle.begin_pool_wait());
            }
            result
        })
        .await
        .map_err(|error| Self::publish_pool_error(error, commands.connect_timeout));
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

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "formats the typed external Redis driver failure; no internal runtime \
                      ownership is inferred"
        )
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: REDIS })
            .attach_printable(error.to_string())
    }

    fn publish_pool_error(
        error: bb8::RunError<redis::RedisError>,
        connect_timeout: ConfiguredDeadline,
    ) -> Report<EmitterWriteError> {
        match error {
            bb8::RunError::User(source) => redis_pool_error(source, connect_timeout)
                .change_context(EmitterWriteError::Publish { connector: REDIS }),
            bb8::RunError::TimedOut => {
                // The emitter shows the message an emitter attached, so the deadline leads.
                let elapsed = connect_timeout.elapsed("checking out a Redis command connection");
                Report::new(elapsed)
                    .change_context(EmitterWriteError::Publish { connector: REDIS })
                    .attach_printable(elapsed.to_string())
            }
        }
    }
}

impl RedisPubSubEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Redis(nervix_models::RedisEmitterEntity::PubSub);
}

#[async_trait]
impl EmitterLifecycle for RedisPubSubEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each `PUBLISH` is answered before the publish returns, so the client holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for RedisPubSubEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let pool = self.pool.pool();
        for record in records {
            nervix_primitives::task::consume_budget().await;
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
                Ok(_) => outcome.deliver(record.id),
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use nervix_connector::DeadlineElapsed;
    use nervix_models::{ClientConfigSchemaError, DeadlineKey};
    use nervix_test_environment::dns_authority::DnsAnswer;

    use super::*;
    use crate::pubsub::{
        counting_redis::{ConnectionCounts, CountingRedis},
        test_fixture::Fixture,
    };

    /// The deadline a client that leaves `connect_timeout_ms` out connects within.
    fn default_connect_timeout() -> ConfiguredDeadline {
        ConfiguredDeadline::connect(&[]).expect("a client without the key takes the default")
    }

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

        assert!(RedisPubSubEmitter::is_record_failure(&rejected));
        assert!(!RedisPubSubEmitter::is_record_failure(&ambiguous));
        assert!(!RedisPubSubEmitter::is_record_failure(&disconnected));
    }

    #[nervix_primitives::test]
    async fn command_connections_receive_all_answers_from_the_node_resolver() {
        let fixture = Fixture::start().await;
        let name = "redis.nervix.test";
        fixture.answer(
            name,
            vec![
                IpAddr::V4(Ipv4Addr::new(127, 0, 7, 1)),
                IpAddr::V4(Ipv4Addr::new(127, 0, 7, 2)),
            ],
        );
        let answers: Vec<_> = RedisDns(fixture.dns.clone())
            .resolve(name, 6379)
            .await
            .expect("the Redis hook receives the fixture answers")
            .collect();
        assert_eq!(
            answers,
            vec![
                SocketAddr::from(([127, 0, 7, 1], 6379)),
                SocketAddr::from(([127, 0, 7, 2], 6379)),
            ]
        );
        assert!(fixture.authority.questions_for(name) > 0);
    }

    #[nervix_primitives::test]
    async fn a_command_connection_failure_keeps_the_typed_dns_cause() {
        let fixture = Fixture::start().await;
        let name = "missing.nervix.test";
        fixture.authority.set(
            name,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let client =
            RedisClient::open(format!("redis://{name}:6379")).expect("the Redis URL is valid");
        let manager = RedisCommandManager {
            client,
            dns: fixture.dns,
            connect_timeout: default_connect_timeout(),
            closures: ConnectionClosures::default(),
        };
        let source = <RedisCommandManager as bb8::ManageConnection>::connect(&manager)
            .await
            .expect_err("an absent name cannot open a command connection");
        assert!(DnsLookupError::find_in(&source).is_some(), "{source:?}");
        let emitter_error = RedisPubSubEmitter::publish_pool_error(
            bb8::RunError::User(source.clone()),
            default_connect_timeout(),
        );
        assert!(matches!(
            emitter_error
                .frames()
                .find_map(|frame| frame.downcast_ref::<RedisClientError>()),
            Some(RedisClientError::Resolve {
                host,
                failure: DnsLookupFailure::NameNotFound
            }) if host == name
        ));
        let error = redis_pool_error(source, default_connect_timeout());
        assert!(matches!(
            error.current_context(),
            RedisClientError::Resolve {
                host,
                failure: DnsLookupFailure::NameNotFound
            } if host == name
        ));
    }

    #[nervix_primitives::test]
    async fn command_connections_classify_empty_and_silent_answers() {
        let fixture = Fixture::start().await;
        for (name, answer, expected) in [
            (
                "empty.nervix.test",
                DnsAnswer::NoAddresses {
                    negative_ttl: Duration::from_secs(1),
                },
                DnsLookupFailure::NoAddresses,
            ),
            (
                "silent.nervix.test",
                DnsAnswer::Silent,
                DnsLookupFailure::Timeout,
            ),
        ] {
            fixture.authority.set(name, answer);
            let client =
                RedisClient::open(format!("redis://{name}:6379")).expect("the Redis URL is valid");
            let manager = RedisCommandManager {
                client,
                dns: fixture.dns.clone(),
                connect_timeout: default_connect_timeout(),
                closures: ConnectionClosures::default(),
            };
            let error = <RedisCommandManager as bb8::ManageConnection>::connect(&manager)
                .await
                .expect_err("the unavailable name cannot open a command connection");
            assert_eq!(
                DnsLookupError::find_in(&error).map(DnsLookupError::failure),
                Some(expected),
                "{name}: {error:?}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn cancelling_a_command_lookup_does_not_hold_the_next_connection() {
        let fixture = Fixture::start().await;
        let name = "changing.nervix.test";
        fixture.authority.set(name, DnsAnswer::Silent);
        let dns = RedisDns(fixture.dns.clone());
        let attempting = nervix_primitives::task::spawn(async move {
            dns.resolve(name, 6379)
                .await
                .map(|answers| answers.collect::<Vec<_>>())
        });
        nervix_primitives::time::timeout(Duration::from_secs(2), async {
            while fixture.authority.questions_for(name) == 0 {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first lookup asks the authority");
        attempting.abort();
        let stopped = match attempting.await {
            Err(stopped) => stopped,
            Ok(_) => panic!("the cancelled lookup stops"),
        };
        assert!(stopped.is_cancelled());
        fixture.answer(name, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let addresses: Vec<_> = nervix_primitives::time::timeout(
            Duration::from_secs(3),
            RedisDns(fixture.dns).resolve(name, 6379),
        )
        .await
        .expect("the next connection is not held by the cancelled lookup")
        .expect("the new answer resolves")
        .collect();
        assert_eq!(addresses, vec![SocketAddr::from(([127, 0, 0, 1], 6379))]);
    }
    #[test]
    fn a_checkout_that_outlasts_connect_timeout_names_the_key_and_its_bound() {
        let connect_timeout = ConfiguredDeadline::connect(&[ClientConfigEntry {
            key: "connect_timeout_ms".to_string(),
            value: "1000".to_string(),
        }])
        .expect("one second is a valid deadline");
        let error =
            RedisPubSubEmitter::publish_pool_error(bb8::RunError::TimedOut, connect_timeout);
        assert!(matches!(
            error.current_context(),
            EmitterWriteError::Publish { connector: REDIS }
        ));
        let elapsed = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .expect("the elapsed deadline stays beneath the publish failure");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_secs(1));
        assert!(format!("{error:#}").contains("within connect_timeout_ms 1s"));
        let leading = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<String>());
        assert_eq!(
            leading.map(String::as_str),
            Some(
                "checking out a Redis command connection did not finish within connect_timeout_ms \
                 1s"
            )
        );
    }

    #[test]
    fn a_connection_the_driver_timed_out_names_connect_timeout_beneath_it() {
        let timed_out = redis::RedisError::from(io::Error::from(io::ErrorKind::TimedOut));
        let error = redis_pool_error(timed_out, default_connect_timeout());
        assert!(matches!(
            error.current_context(),
            RedisClientError::Connect { .. }
        ));
        let elapsed = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .expect("the elapsed deadline stays beneath the connection failure");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_secs(30));

        let refused = redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused));
        let error = redis_pool_error(refused, default_connect_timeout());
        assert!(
            error
                .frames()
                .all(|frame| frame.downcast_ref::<DeadlineElapsed>().is_none()),
            "a refused connection did not run out of its deadline: {error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn an_invalid_connect_timeout_refuses_to_open_the_pool() {
        let fixture = Fixture::start().await;
        for value in ["0", "soon", "-5"] {
            let config = [
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: "redis://127.0.0.1:1".to_string(),
                },
                ClientConfigEntry {
                    key: "connect_timeout_ms".to_string(),
                    value: value.to_string(),
                },
            ];
            let error = open_redis_command_pool(
                &config,
                ClientPoolBounds::new(0, std::num::NonZeroU32::MIN)
                    .expect("an empty minimum fits any maximum"),
                fixture.dns.clone(),
            )
            .await
            .expect_err("an invalid deadline cannot open a pool");
            assert!(
                matches!(
                    error.current_context(),
                    RedisClientError::InvalidConnectTimeout
                ),
                "{value}: {error:?}"
            );
        }
    }

    /// The pool a client opens with `entries` beside an address nothing listens on. Its minimum is
    /// zero, so it opens without a connection.
    async fn open_unconnected_pool(
        fixture: &Fixture,
        entries: &[(&str, &str)],
    ) -> Result<RedisCommandPool, Report<RedisClientError>> {
        let mut config = vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "redis://127.0.0.1:1".to_string(),
        }];
        for (key, value) in entries {
            config.push(ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            });
        }
        open_redis_command_pool(
            &config,
            ClientPoolBounds::new(0, std::num::NonZeroU32::MIN)
                .expect("an empty minimum fits any maximum"),
            fixture.dns.clone(),
        )
        .await
    }

    #[nervix_primitives::test]
    async fn a_pool_without_lifetime_keys_takes_the_declared_defaults() {
        let fixture = Fixture::start().await;
        let pool = open_unconnected_pool(&fixture, &[])
            .await
            .expect("a pool with no minimum opens without a connection");
        let config = pool.shared.commands.connections.config();
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(10 * 60)));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(config.connection_timeout, default_connect_timeout().bound());
        // The driver's idle minimum would open a connection beside every checkout.
        assert_eq!(config.min_idle, None);
        // A minimum of zero has nothing to keep.
        assert!(pool.shared._minimum.is_none());
    }

    #[nervix_primitives::test]
    async fn a_pool_takes_the_lifetimes_its_client_declares() {
        let fixture = Fixture::start().await;
        let pool = open_unconnected_pool(
            &fixture,
            &[
                ("pool_idle_timeout_ms", "45000"),
                ("pool_max_lifetime_ms", "5000"),
            ],
        )
        .await
        .expect("a pool with no minimum opens without a connection");
        let config = pool.shared.commands.connections.config();
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(45)));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(5)));
    }

    #[nervix_primitives::test]
    async fn an_invalid_pool_lifetime_refuses_to_open_the_pool() {
        let fixture = Fixture::start().await;
        for key in ["pool_idle_timeout_ms", "pool_max_lifetime_ms"] {
            for value in ["0", "soon", "-5"] {
                let error = open_unconnected_pool(&fixture, &[(key, value)])
                    .await
                    .expect_err("an invalid lifetime cannot open a pool");
                assert!(
                    matches!(
                        error.current_context(),
                        RedisClientError::InvalidPoolLifetime { key: refused } if *refused == key
                    ),
                    "{key} = {value}: {error:?}"
                );
                let cause = error
                    .frames()
                    .find_map(|frame| frame.downcast_ref::<ClientConfigSchemaError>());
                assert!(
                    matches!(
                        cause,
                        Some(ClientConfigSchemaError::InvalidValue { key: refused, .. })
                            if refused == key
                    ),
                    "{key} = {value}: {error:?}"
                );
            }
        }
    }

    /// How long a check waits for a connection that must never arrive. Lengthening it only
    /// strengthens the check.
    const NO_FURTHER_CONNECTION: Duration = Duration::from_millis(500);

    /// How long a check waits for the stand-in's counts. Generous, because the wait ends as soon as
    /// they reach what the check expects.
    const COUNTS_REACHED: Duration = Duration::from_secs(30);

    /// The configuration of a client whose only key is the stand-in's address.
    fn counted_client(redis: &CountingRedis) -> [ClientConfigEntry; 1] {
        [ClientConfigEntry {
            key: "addr".to_string(),
            value: redis.url(),
        }]
    }

    #[nervix_primitives::test]
    async fn a_publisher_checking_connections_out_opens_none_beyond_the_minimum() {
        let fixture = Fixture::start().await;
        let redis = CountingRedis::start().await;
        let pool = open_redis_command_pool(
            &counted_client(&redis),
            ClientPoolBounds::new(1, std::num::NonZeroU32::new(4).expect("four is positive"))
                .expect("a minimum of one fits a maximum of four"),
            fixture.dns.clone(),
        )
        .await
        .expect("the stand-in accepts the pool's minimum");
        assert_eq!(redis.counts().accepted, 1);
        for _ in 0..5 {
            let mut connection = pool
                .shared
                .commands
                .connections
                .get()
                .await
                .expect("the pool hands its idle connection over");
            let _subscribers: i64 = connection
                .publish("events", b"payload".as_slice())
                .await
                .expect("the stand-in answers the publish");
        }
        let further = nervix_primitives::time::timeout(
            NO_FURTHER_CONNECTION,
            redis.reach(
                COUNTS_REACHED,
                "a connection beyond the minimum",
                |counts| counts.accepted > 1,
            ),
        )
        .await;
        assert!(
            further.is_err(),
            "one publisher at a time made the pool open a connection beyond its minimum: {:?}",
            redis.counts()
        );
        assert_eq!(pool.shared.commands.connections.state().connections, 1);
    }

    /// Open a pool of `bounds` against the stand-in, with `entries` beside its address.
    async fn open_counted_pool(
        fixture: &Fixture,
        redis: &CountingRedis,
        bounds: ClientPoolBounds,
        entries: &[(&str, &str)],
    ) -> RedisCommandPool {
        let mut config = counted_client(redis).to_vec();
        for (key, value) in entries {
            config.push(ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            });
        }
        open_redis_command_pool(&config, bounds, fixture.dns.clone())
            .await
            .expect("the stand-in accepts the pool's minimum")
    }

    fn bounds(minimum: u32, maximum: u32) -> ClientPoolBounds {
        let maximum = std::num::NonZeroU32::new(maximum).expect("the test maximum is positive");
        ClientPoolBounds::new(minimum, maximum).expect("the test minimum fits its maximum")
    }

    #[nervix_primitives::test]
    async fn opening_a_pool_establishes_its_minimum() {
        let fixture = Fixture::start().await;
        let redis = CountingRedis::start().await;
        let pool = open_counted_pool(&fixture, &redis, bounds(3, 4), &[]).await;
        assert!(pool.shared._minimum.is_some());
        let state = pool.shared.commands.connections.state();
        assert_eq!((state.connections, state.idle_connections), (3, 3));
        assert_eq!(
            redis
                .reach(COUNTS_REACHED, "three open connections", |counts| counts
                    .open
                    == 3)
                .await,
            ConnectionCounts {
                accepted: 3,
                open: 3
            }
        );
    }

    #[nervix_primitives::test]
    async fn a_closed_connection_is_replaced_counting_the_ones_checked_out() {
        let lifetime = Duration::from_millis(250);
        let fixture = Fixture::start().await;
        let redis = CountingRedis::start().await;
        let lifetime_ms = lifetime.as_millis().to_string();
        let pool = open_counted_pool(
            &fixture,
            &redis,
            bounds(2, 4),
            &[("pool_max_lifetime_ms", lifetime_ms.as_str())],
        )
        .await;
        let opened_at = Instant::now();
        let connections = &pool.shared.commands.connections;
        let held = connections
            .get()
            .await
            .expect("the pool hands its first idle connection over");
        let returned = connections
            .get()
            .await
            .expect("the pool hands its second idle connection over");
        // Both were opened before the pool was, so this is when both have outlived the lifetime.
        nervix_primitives::time::sleep_until(opened_at + lifetime).await;
        // Returned past its lifetime, this one is closed while the other stays checked out.
        drop(returned);
        let counts = redis
            .reach(
                COUNTS_REACHED,
                "one replacement beside the checked-out connection",
                |counts| counts.accepted == 3 && counts.open == 2,
            )
            .await;
        assert_eq!(
            counts,
            ConnectionCounts {
                accepted: 3,
                open: 2
            }
        );
        let further = nervix_primitives::time::timeout(
            NO_FURTHER_CONNECTION,
            redis.reach(COUNTS_REACHED, "a second replacement", |counts| {
                counts.accepted > 3
            }),
        )
        .await;
        assert!(
            further.is_err(),
            "the pool replaced its closed connection beside the checked-out one with more than \
             one: {:?}",
            redis.counts()
        );
        let state = connections.state();
        assert_eq!((state.connections, state.idle_connections), (2, 1));
        drop(held);
    }

    #[nervix_primitives::test]
    async fn a_minimum_that_cannot_be_opened_fails_the_open_with_the_connection_cause() {
        let fixture = Fixture::start().await;
        let name = "missing.nervix.test";
        fixture.authority.set(
            name,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let missing = [ClientConfigEntry {
            key: "addr".to_string(),
            value: format!("redis://{name}:6379"),
        }];
        let error = open_redis_command_pool(&missing, bounds(1, 4), fixture.dns.clone())
            .await
            .expect_err("a missing name cannot open the pool's minimum");
        assert!(
            matches!(
                error.current_context(),
                RedisClientError::Resolve {
                    host,
                    failure: DnsLookupFailure::NameNotFound
                } if host == name
            ),
            "{error:?}"
        );
        let refused = [ClientConfigEntry {
            key: "addr".to_string(),
            value: "redis://127.0.0.1:1".to_string(),
        }];
        let error = open_redis_command_pool(&refused, bounds(1, 4), fixture.dns.clone())
            .await
            .expect_err("a refused address cannot open the pool's minimum");
        assert!(
            matches!(error.current_context(), RedisClientError::Connect { .. }),
            "{error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn dropping_the_pool_stops_its_keeper_and_closes_its_connections() {
        let fixture = Fixture::start().await;
        let redis = CountingRedis::start().await;
        let pool = open_counted_pool(&fixture, &redis, bounds(2, 4), &[]).await;
        assert_eq!(
            redis.counts(),
            ConnectionCounts {
                accepted: 2,
                open: 2
            }
        );
        drop(pool);
        let counts = redis
            .reach(
                COUNTS_REACHED,
                "every connection closed with the pool",
                |counts| counts.open == 0,
            )
            .await;
        assert_eq!(counts.accepted, 2);
    }

    #[nervix_primitives::test]
    async fn a_restoration_that_cannot_check_out_names_the_minimum_above_its_cause() {
        let fixture = Fixture::start().await;
        // Nothing listens on the address, so every connection the pool opens is refused.
        let pool = open_unconnected_pool(&fixture, &[("connect_timeout_ms", "200")])
            .await
            .expect("a pool with no minimum opens without a connection");
        let commands = &pool.shared.commands;
        let restoration = Restoration {
            minimum: std::num::NonZeroU32::MIN,
            checkouts: std::num::NonZeroU32::MIN,
        };
        let error = commands
            .restore(restoration)
            .await
            .expect_err("a pool that cannot open a connection cannot restore its minimum");
        assert!(
            matches!(
                error.current_context(),
                PoolMinimumError::NotRestored { minimum } if *minimum == restoration.minimum
            ),
            "{error:?}"
        );
        let elapsed = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .expect("the checkout's elapsed deadline stays beneath the restoration");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));

        let refused = redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused));
        let error = commands.restoration_error(bb8::RunError::User(refused), restoration);
        assert!(matches!(
            error.current_context(),
            PoolMinimumError::NotRestored { .. }
        ));
        assert!(
            error.frames().any(|frame| matches!(
                frame.downcast_ref::<RedisClientError>(),
                Some(RedisClientError::Connect { .. })
            )),
            "{error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn a_pooled_connection_carries_pipelines_and_names_its_database() {
        let fixture = Fixture::start().await;
        let redis = CountingRedis::start().await;
        let pool = open_counted_pool(&fixture, &redis, bounds(1, 1), &[]).await;
        let mut connection = pool
            .shared
            .commands
            .connections
            .get()
            .await
            .expect("the pool hands its connection over");
        let answers: (String, String) = redis::pipe()
            .cmd("PING")
            .cmd("PING")
            .query_async(&mut *connection)
            .await
            .expect("the stand-in answers both commands of the pipeline");
        assert_eq!(answers, ("PONG".to_string(), "PONG".to_string()));
        assert_eq!(connection.get_db(), 0);
    }
}
