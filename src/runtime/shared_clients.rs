//! One live connector per named client on this node, shared by every local user of that client.
//!
//! Layer: data plane.
//! Owns: the connector instance behind each named client on this node, the interest its local
//! users hold in it, and the wait a user sits in between asking for a connection and being handed
//! one.
//! Depends on: the vocabulary that names a domain and a client, the client Models an instance is
//! built from, the resolved client configuration, and the connector drivers.
//! Must not know: which graph node asked for a connection, how a batch is built, or how a
//! schedule was decided.
//!
//! A pool belongs to one named client in one domain on one physical node. Every local emitter and
//! ingestor of that client borrows from the same instance, which is what makes the declared bounds
//! a property of the transport rather than of whoever happened to open a connection first.

use error_stack::Report;
use url::Url;

use super::*;
use crate::runtime::{
    emitters::{MongoDbClient, MySqlPool, MySqlSharedPool, PgPool, RedisCommandPool},
    planning::{PooledClientPlan, PooledTransport},
};

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
    #[error("client '{client}' does not own a connection pool")]
    NotPooled { client: String },
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
    MySql(MySqlSharedPool),
    MongoDb(MongoDbClient),
    Redis(RedisCommandPool),
    Postgres(PgPool),
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
            SharedClientInstance::MySql(shared) => Ok(shared.pool()),
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
    ) -> Result<&PgPool, Report<SharedClientError>> {
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
    instance: StdArc<tokio::sync::OnceCell<StdArc<SharedClient>>>,
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

/// Marks its graph node as waiting for a connection until it is dropped.
///
/// Acquisition can end in a connection, a timeout, or task cancellation, and the wait has to clear
/// in all three. Tying it to a guard is what makes that true without every call site remembering.
pub(in crate::runtime) struct PoolWaitGuard {
    runtime: Runtime,
    key: DomainNodeRef,
}

impl Drop for PoolWaitGuard {
    fn drop(&mut self) {
        self.runtime.inner.pool_waits.remove(&self.key);
    }
}

impl Runtime {
    /// This node's shared instance of `client`, opening it on first local use.
    ///
    /// The instance is built from the first user's resolved configuration. Every pool-capable
    /// client renders the same configuration for all of its users, so the instance a later user
    /// joins is the one its own configuration describes.
    pub(in crate::runtime) async fn lease_shared_client(
        &self,
        domain: &DomainName,
        name: &ClientName,
        model: &Model,
        resolved: Option<&ResolvedClientConfig>,
    ) -> Result<SharedClientLease, Report<SharedClientError>> {
        let Some(plan) = PooledClientPlan::for_model(model) else {
            return Err(Report::new(SharedClientError::NotPooled {
                client: name.as_str().to_string(),
            }));
        };
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Client, name.clone());
        let cell = {
            let mut slot = self
                .inner
                .shared_clients
                .entry(key.clone())
                .or_insert_with(|| SharedClientSlot {
                    instance: StdArc::new(tokio::sync::OnceCell::new()),
                    users: 0,
                });
            slot.users = slot
                .users
                .checked_add(1)
                .assured("a node holds far fewer clients open than usize can count");
            StdArc::clone(&slot.instance)
        };

        let opened = cell
            .get_or_try_init(|| Self::open_shared_client(name, &plan, resolved))
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
        name: &ClientName,
        plan: &PooledClientPlan<'_>,
        resolved: Option<&ResolvedClientConfig>,
    ) -> Result<StdArc<SharedClient>, Report<SharedClientError>> {
        let mounts = resolved.and_then(|resolved| resolved.mounts.clone());
        let config = client_config_entries(resolved, plan.config);
        let transport: &'static str = match plan.transport {
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
        let instance = match plan.transport {
            PooledTransport::Postgres => SharedClientInstance::Postgres(
                emitters::open_postgres_pool(config, plan.bounds)
                    .await
                    .map_err(opened)?,
            ),
            PooledTransport::MySql => SharedClientInstance::MySql(
                emitters::open_mysql_pool(config, plan.bounds)
                    .await
                    .map_err(opened)?,
            ),
            PooledTransport::MongoDb => SharedClientInstance::MongoDb(
                emitters::open_mongodb_client(config, plan.bounds)
                    .await
                    .map_err(opened)?,
            ),
            PooledTransport::Redis => SharedClientInstance::Redis(
                emitters::open_redis_command_pool(config, plan.bounds)
                    .await
                    .map_err(opened)?,
            ),
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

    /// Mark `waiter` as waiting for a connection from `client` until the returned guard is dropped.
    pub(in crate::runtime) fn pool_wait_guard(
        &self,
        waiter: &DomainNodeRef,
        client: &ClientName,
    ) -> PoolWaitGuard {
        self.inner.pool_waits.insert(
            waiter.clone(),
            PoolWait {
                client: client.clone(),
                since: Instant::now(),
            },
        );
        PoolWaitGuard {
            runtime: self.clone(),
            key: waiter.clone(),
        }
    }

    /// What `waiter` is waiting for, when it has asked for a connection and not yet been given one.
    pub(in crate::runtime) fn pool_wait(&self, waiter: &DomainNodeRef) -> Option<PoolWait> {
        self.inner
            .pool_waits
            .get(waiter)
            .map(|wait| wait.value().clone())
    }
}
