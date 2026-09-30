//! One live connector per named client on this node, shared by every local user of that client.
//!
//! Layer: data plane.
//! Owns: the connector instance behind each named client on this node, the interest its local
//! users hold in it, and the wait a user sits in between asking for a connection and being handed
//! one.
//! Depends on: the vocabulary that names a domain and a client, the pooled client a sink's start
//! plan decides, the resolved client configuration, and the connector drivers.
//! Must not know: which graph node asked for a connection, how a batch is built, or how a
//! schedule was decided.
//!
//! A pool belongs to one named client in one domain on one physical node. Every local emitter and
//! ingestor of that client borrows from the same instance, which is what makes the declared bounds
//! a property of the transport rather than of whoever happened to open a connection first.

use error_stack::Report;
use nervix_connector::{ClientResourceMounts, ResolvedClientConfig, SinkStartError};
use nervix_connector_mongodb::MongoDbClient;
use nervix_connector_mysql::MySqlPool;
use nervix_connector_postgres::PostgresPool;
use nervix_connector_redis::{RedisClientError, RedisCommandPool, open_redis_command_pool};
use url::Url;

use super::*;

/// Why one client's connector instance could not be opened.
///
/// The transport names itself so a diagnostic says which driver refused, and no variant carries a
/// credential or a complete connection address.
#[derive(Debug, thiserror::Error)]
pub(in crate::runtime) enum OpenClientError {
    #[error("missing {transport} client config key '{key}'")]
    MissingConfig {
        transport: &'static str,
        key: &'static str,
    },
    #[error("invalid {transport} client configuration: {reason}")]
    InvalidConfig {
        transport: &'static str,
        reason: String,
    },
    #[error("failed to connect to {transport}: {reason}")]
    Connect {
        transport: &'static str,
        reason: String,
    },
}

impl OpenClientError {
    /// How the Redis connector's own client failure reads as a client this node could not open.
    fn from_redis(error: Report<RedisClientError>) -> Report<Self> {
        let context = match error.current_context() {
            RedisClientError::MissingConfig { key } => Self::MissingConfig {
                transport: "Redis",
                key,
            },
            RedisClientError::InvalidConfig { reason } => Self::InvalidConfig {
                transport: "Redis",
                reason: reason.clone(),
            },
            RedisClientError::Connect { reason } => Self::Connect {
                transport: "Redis",
                reason: reason.clone(),
            },
            RedisClientError::Resolve { .. } => Self::Connect {
                transport: "Redis",
                reason: error.current_context().to_string(),
            },
        };
        error.change_context(context)
    }
}

/// Configuration keys and address parameters that would size a pool behind NSPL's back.
///
/// Each driver reads at least one of these: `mysql_async` takes `pool_min`/`pool_max` from its URL
/// and the MongoDB driver takes `minPoolSize`/`maxPoolSize` from its own. Whichever won would be a
/// silent, per-driver answer to a question the client already answers, so both are refused.
const POOL_SIZING_KEYS: &[&str] = &[
    "maxpoolsize",
    "max_pool_size",
    "minpoolsize",
    "min_pool_size",
    "pool_max",
    "pool_min",
    "pool_size",
];

/// Reject connector configuration that tries to size the connection pool.
///
/// Capacity is declared once, in the client's `POOL SIZE` clause. A raw entry or address parameter
/// that also sizes the pool is rejected rather than ignored, so an operator never has two
/// disagreeing answers with the winner decided by which driver read which.
fn reject_pool_sizing(
    transport: &'static str,
    config: &[nervix_models::ClientConfigEntry],
) -> Result<(), Report<OpenClientError>> {
    let rejected = |key: &str| {
        Report::new(OpenClientError::InvalidConfig {
            transport,
            reason: format!(
                "'{key}' sizes the connection pool, which is declared by POOL SIZE MIN ... MAX \
                 ... on the client"
            ),
        })
    };
    for entry in config {
        if POOL_SIZING_KEYS.contains(&entry.key.to_ascii_lowercase().as_str()) {
            return Err(rejected(&entry.key));
        }
        // Only an address carries query parameters, and only its parameters can reach a driver's
        // own sizing; other values are opaque strings the driver never parses that way.
        if !entry.key.eq_ignore_ascii_case("addr") {
            continue;
        }
        let Ok(url) = Url::parse(&entry.value) else {
            continue;
        };
        for (key, _) in url.query_pairs() {
            if POOL_SIZING_KEYS.contains(&key.to_ascii_lowercase().as_str()) {
                return Err(rejected(&key));
            }
        }
    }
    Ok(())
}

/// Why this node could not supply a shared client.
#[derive(Debug, thiserror::Error)]
pub(in crate::runtime) enum SharedClientError {
    #[error("failed to open client '{client}'")]
    Open { client: String },
    #[error("client '{client}' is not a {expected} client")]
    WrongTransport {
        client: String,
        expected: &'static str,
    },
}

/// The driver-owned instance behind one named client.
///
/// Each variant pairs a transport with the widest thing its driver can share, so a holder cannot
/// carry a MySQL pool while believing it has a MongoDB client. Only transports that own a real
/// connection pool appear here; the connection policies of the broker, socket and request
/// transports stay with those connectors.
pub(in crate::runtime) enum SharedClientInstance {
    MySql(MySqlPool),
    MongoDb(MongoDbClient),
    Redis(RedisCommandPool),
    Postgres(PostgresPool),
}

/// One named client's instance on this node, together with everything it needs to stay usable.
pub(in crate::runtime) struct SharedClient {
    instance: SharedClientInstance,
    /// The rendered resource mount this instance reads its TLS material from.
    ///
    /// The instance outlives every individual user, so the mount is held here rather than by the
    /// user that happened to resolve it first, and is released when the instance closes.
    _mounts: Option<Arc<ClientResourceMounts>>,
}

impl SharedClient {
    /// The MySQL pool behind this client, or an error naming the transport mismatch.
    pub(in crate::runtime) fn mysql(
        &self,
        client: &ClientName,
    ) -> Result<&MySqlPool, Report<SharedClientError>> {
        match &self.instance {
            SharedClientInstance::MySql(pool) => Ok(pool),
            _ => Err(Report::new(SharedClientError::WrongTransport {
                client: client.as_str().to_string(),
                expected: "MySQL",
            })),
        }
    }

    /// The MongoDB client behind this client, or an error naming the transport mismatch.
    pub(in crate::runtime) fn mongodb(
        &self,
        client: &ClientName,
    ) -> Result<&MongoDbClient, Report<SharedClientError>> {
        match &self.instance {
            SharedClientInstance::MongoDb(driver) => Ok(driver),
            _ => Err(Report::new(SharedClientError::WrongTransport {
                client: client.as_str().to_string(),
                expected: "MongoDB",
            })),
        }
    }

    /// The Postgres pool behind this client, or an error naming the transport mismatch.
    pub(in crate::runtime) fn postgres(
        &self,
        client: &ClientName,
    ) -> Result<&PostgresPool, Report<SharedClientError>> {
        match &self.instance {
            SharedClientInstance::Postgres(pool) => Ok(pool),
            _ => Err(Report::new(SharedClientError::WrongTransport {
                client: client.as_str().to_string(),
                expected: "Postgres",
            })),
        }
    }

    /// The Redis command pool behind this client, or an error naming the transport mismatch.
    pub(in crate::runtime) fn redis(
        &self,
        client: &ClientName,
    ) -> Result<&RedisCommandPool, Report<SharedClientError>> {
        match &self.instance {
            SharedClientInstance::Redis(pool) => Ok(pool),
            _ => Err(Report::new(SharedClientError::WrongTransport {
                client: client.as_str().to_string(),
                expected: "Redis",
            })),
        }
    }
}

/// One slot of the registry: the instance and the number of local users holding it open.
pub(in crate::runtime) struct SharedClientSlot {
    /// Built once however many users race to be first, so a burst of emitters starting together
    /// opens one pool rather than one pool each.
    instance: StdArc<nervix_primitives::sync::OnceCell<StdArc<SharedClient>>>,
    users: usize,
}

/// One local user's interest in this node's shared instance of a client.
///
/// A lease is held for exactly as long as its emitter or ingestor can use the client. The instance
/// closes when the last lease is dropped, so removing one user preserves the pool for the others
/// and removing the last one releases the connections and the resource mount together.
pub(in crate::runtime) struct SharedClientLease {
    runtime: Runtime,
    key: DomainNodeRef,
    client: StdArc<SharedClient>,
}

impl SharedClientLease {
    pub(in crate::runtime) fn client(&self) -> &SharedClient {
        &self.client
    }
}

impl Drop for SharedClientLease {
    fn drop(&mut self) {
        self.runtime.release_shared_client(&self.key);
    }
}

/// What a graph node is waiting for while it holds no connection yet.
///
/// `DESCRIBE` reads this, so an emitter blocked on a full pool reports the wait instead of looking
/// idle next to a connection it does not have.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct PoolWait {
    pub(in crate::runtime) client: ClientName,
    pub(in crate::runtime) since: Instant,
}

impl PoolWait {
    /// How this wait reads in `DESCRIBE`, naming the client and how long the node has waited.
    pub(in crate::runtime) fn describe(&self) -> String {
        let waited = Duration::from_secs(self.since.elapsed().as_secs());
        format!(
            "waiting for a connection from client '{}' for {}",
            self.client.as_str(),
            humantime::format_duration(waited)
        )
    }
}

/// Publication retained by one pooled sink. Its connector serializes connection borrows.
pub(in crate::runtime) struct PoolWaitSlot {
    client: ClientName,
    active: ArcSwapOption<PoolWait>,
}

impl PoolWaitSlot {
    fn new(client: ClientName) -> Self {
        Self {
            client,
            active: ArcSwapOption::empty(),
        }
    }

    pub(in crate::runtime) fn begin(slot: &Arc<Self>) -> PoolWaitGuard {
        slot.active.store(Some(StdArc::new(PoolWait {
            client: slot.client.clone(),
            since: Instant::now(),
        })));
        PoolWaitGuard { slot: slot.clone() }
    }

    /// A ready borrow performs no publication or registry operation. Cancellation drops its guard.
    pub(in crate::runtime) async fn borrow<F: std::future::Future>(
        slot: &Arc<Self>,
        future: F,
    ) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut waiting = None;
        std::future::poll_fn(|context| {
            let result = future.as_mut().poll(context);
            if result.is_pending() && waiting.is_none() {
                waiting = Some(Self::begin(slot));
            }
            result
        })
        .await
    }
}

/// Clears a pending connection borrow on success, failure or cancellation.
pub(in crate::runtime) struct PoolWaitGuard {
    slot: Arc<PoolWaitSlot>,
}

impl Drop for PoolWaitGuard {
    fn drop(&mut self) {
        self.slot.active.store(None);
    }
}

/// Registry interest follows the sink lifetime, independently of individual connection borrows.
pub(in crate::runtime) struct PoolWaitRegistration {
    pub(in crate::runtime) slot: Arc<PoolWaitSlot>,
    runtime: Runtime,
    key: DomainNodeRef,
}

impl Drop for PoolWaitRegistration {
    fn drop(&mut self) {
        self.runtime
            .inner
            .pool_waits
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.slot));
    }
}

impl Runtime {
    /// This node's shared instance of `client`, opening it on first local use as `pool` plans.
    ///
    /// The instance is built from the first user's resolved configuration. Every pool-capable
    /// client renders the same configuration for all of its users, so the instance a later user
    /// joins is the one its own configuration describes.
    pub(in crate::runtime) async fn lease_shared_client(
        &self,
        domain: &DomainName,
        client: &EmitterClientSpec,
        pool: PooledClientPlan,
    ) -> Result<SharedClientLease, Report<SharedClientError>> {
        let name = &client.name;
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Client, name.clone());
        let cell = {
            let mut slot = self
                .inner
                .shared_clients
                .entry(key.clone())
                .or_insert_with(|| SharedClientSlot {
                    instance: StdArc::new(nervix_primitives::sync::OnceCell::new()),
                    users: 0,
                });
            slot.users = slot
                .users
                .checked_add(1)
                .assured("a node holds far fewer clients open than usize can count");
            StdArc::clone(&slot.instance)
        };

        let opened = cell
            .get_or_try_init(|| self.open_shared_client(name, pool, &client.config))
            .await;

        match opened {
            Ok(client) => Ok(SharedClientLease {
                runtime: self.clone(),
                key,
                client: StdArc::clone(client),
            }),
            Err(error) => {
                // The interest was taken before the open was attempted, so it has to be given back
                // here; a failed open leaves the slot empty and the next user retries it.
                self.release_shared_client(&key);
                Err(error)
            }
        }
    }

    /// Build the driver instance behind one pool-capable client.
    async fn open_shared_client(
        &self,
        name: &ClientName,
        pool: PooledClientPlan,
        resolved: &ResolvedClientConfig,
    ) -> Result<StdArc<SharedClient>, Report<SharedClientError>> {
        let mounts = resolved.mounts.clone();
        let config = resolved.entries.as_slice();
        let transport: &'static str = match pool.transport {
            PooledTransport::Postgres => "Postgres",
            PooledTransport::MySql => "MySQL",
            PooledTransport::MongoDb => "MongoDB",
            PooledTransport::Redis => "Redis",
        };
        reject_pool_sizing(transport, config).map_err(|error| {
            error.change_context(SharedClientError::Open {
                client: name.as_str().to_string(),
            })
        })?;
        let opened = |error: Report<OpenClientError>| {
            error.change_context(SharedClientError::Open {
                client: name.as_str().to_string(),
            })
        };
        // A connector states its own start failure, and the client names which one could not open.
        let started = |error: Report<SinkStartError>| {
            error.change_context(SharedClientError::Open {
                client: name.as_str().to_string(),
            })
        };
        let instance = match pool.transport {
            PooledTransport::Postgres => SharedClientInstance::Postgres(
                PostgresPool::open(config, pool.bounds)
                    .await
                    .map_err(started)?,
            ),
            PooledTransport::MySql => SharedClientInstance::MySql(
                MySqlPool::open(config, pool.bounds)
                    .await
                    .map_err(started)?,
            ),
            PooledTransport::MongoDb => SharedClientInstance::MongoDb(
                MongoDbClient::open(config, pool.bounds)
                    .await
                    .map_err(started)?,
            ),
            PooledTransport::Redis => {
                let Some(dns) = self.dns() else {
                    return Err(Report::new(OpenClientError::InvalidConfig {
                        transport: "Redis",
                        reason: "the node DNS resolver is not installed".to_string(),
                    })
                    .change_context(SharedClientError::Open {
                        client: name.as_str().to_string(),
                    }));
                };
                SharedClientInstance::Redis(
                    open_redis_command_pool(config, pool.bounds, dns.clone())
                        .await
                        .map_err(OpenClientError::from_redis)
                        .map_err(opened)?,
                )
            }
        };
        Ok(StdArc::new(SharedClient {
            instance,
            _mounts: mounts,
        }))
    }

    /// Give back one user's interest, closing the instance once the last one leaves.
    fn release_shared_client(&self, key: &DomainNodeRef) {
        let closed = {
            let Some(mut slot) = self.inner.shared_clients.get_mut(key) else {
                return;
            };
            slot.users = slot
                .users
                .checked_sub(1)
                .verified("a lease is created only after its slot took an interest");
            slot.users == 0
        };
        if closed {
            self.inner.shared_clients.remove(key);
        }
    }

    /// Register one pooled sink's stable wait publication for observers.
    pub(in crate::runtime) fn register_pool_wait(
        &self,
        waiter: DomainNodeRef,
        client: ClientName,
    ) -> PoolWaitRegistration {
        let slot = Arc::new(PoolWaitSlot::new(client));
        self.inner.pool_waits.insert(waiter.clone(), slot.clone());
        PoolWaitRegistration {
            slot,
            runtime: self.clone(),
            key: waiter,
        }
    }

    pub(in crate::runtime) fn pool_wait(&self, waiter: &DomainNodeRef) -> Option<PoolWait> {
        let slot = self.inner.pool_waits.get(waiter)?;
        let wait = slot.active.load_full()?;
        Some((*wait).clone())
    }
}

#[cfg(test)]
mod tests {
    use nervix_dns::DnsLookupFailure;
    use nervix_models::ClientPoolBounds;

    use super::*;

    #[nervix_primitives::test]
    async fn retained_pool_wait_reports_only_pending_borrows_and_clears_on_cancel() {
        let runtime = Runtime::new();
        let key = DomainNodeRef::node_in(
            domain("pool_wait"),
            ModelKind::Emitter,
            named::<EmitterName>("sink"),
        );
        let registration = runtime.register_pool_wait(key.clone(), named("database"));
        let slot = registration.slot.clone();
        for _ in 0..100 {
            assert_eq!(PoolWaitSlot::borrow(&slot, std::future::ready(7)).await, 7);
        }
        assert!(slot.active.load().is_none());
        let (sender, receiver) = oneshot::channel::<u8>();
        let mut borrow = Box::pin(PoolWaitSlot::borrow(&slot, receiver));
        std::future::poll_fn(|context| {
            assert!(borrow.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let waiting = runtime
            .pool_wait(&key)
            .assured("the pending borrow is visible to DESCRIBE");
        assert_eq!(waiting.client, named::<ClientName>("database"));
        sender.send(9).assured("the borrow retains its receiver");
        assert_eq!(borrow.await.assured("the connection borrow succeeds"), 9);
        assert!(slot.active.load().is_none());
        let mut borrow = Box::pin(PoolWaitSlot::borrow(&slot, std::future::pending::<()>()));
        std::future::poll_fn(|context| {
            assert!(borrow.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(slot.active.load().is_some());
        drop(borrow);
        assert!(slot.active.load().is_none());
        drop(registration);
        assert!(runtime.pool_wait(&key).is_none());
    }

    #[test]
    fn pool_registration_teardown_preserves_a_replacement_sink() {
        let runtime = Runtime::new();
        let key = DomainNodeRef::node_in(
            domain("pool_wait"),
            ModelKind::Emitter,
            named::<EmitterName>("sink"),
        );
        let first = runtime.register_pool_wait(key.clone(), named("database"));
        let replacement = runtime.register_pool_wait(key.clone(), named("database"));
        drop(first);
        let waiting = PoolWaitSlot::begin(&replacement.slot);
        assert!(runtime.pool_wait(&key).is_some());
        drop(waiting);
        drop(replacement);
        assert!(!runtime.inner.pool_waits.contains_key(&key));
    }

    #[test]
    fn redis_lookup_failure_keeps_its_cause_when_a_shared_client_reports_it() {
        let error = OpenClientError::from_redis(Report::new(RedisClientError::Resolve {
            host: "missing.nervix.test".to_string(),
            failure: DnsLookupFailure::NameNotFound,
        }));
        assert!(matches!(
            error.current_context(),
            OpenClientError::Connect {
                transport: "Redis",
                reason,
            } if reason.contains("missing.nervix.test") && reason.contains("the name does not exist")
        ));
        assert!(
            error
                .frames()
                .any(|frame| frame.downcast_ref::<RedisClientError>().is_some())
        );
    }

    #[nervix_primitives::test]
    async fn redis_pool_reports_a_missing_node_resolver_before_opening() {
        let client = ClientName::try_from("cache".to_string()).assured("the fixture name is valid");
        let error = Runtime::default()
            .open_shared_client(
                &client,
                PooledClientPlan {
                    transport: PooledTransport::Redis,
                    bounds: ClientPoolBounds::new(0, nonzero_ext::nonzero!(1u32))
                        .assured("zero is below one"),
                },
                &ResolvedClientConfig::default(),
            )
            .await
            .err()
            .assured("a Redis pool requires the node resolver");
        assert!(matches!(
            error.current_context(),
            SharedClientError::Open { client } if client == "cache"
        ));
        let cause = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<OpenClientError>())
            .assured("the shared-client error retains the Redis configuration failure");
        assert!(matches!(
            cause,
            OpenClientError::InvalidConfig {
                transport: "Redis",
                reason,
            } if reason == "the node DNS resolver is not installed"
        ));
    }
}
