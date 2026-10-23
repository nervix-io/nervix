//! The node's pooled clients, as the connectors that borrow from them see them.
//!
//! Layer: data plane.
//! - **Owns.** The lease one pooled emitter holds on its client's node-wide instance, the wait it
//!   records while it has asked for a connection and not been handed one, and the connection
//!   sources its connector calls.
//! - **Depends on.** The shared client registry, the vocabulary that names a domain, a client and
//!   a graph node, and the pooled connector crates.
//! - **Must not know.** How a batch is mapped, how a row is encoded, or what an emitter writes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use async_trait::async_trait;
use nervix_connector::{EmitterWriteError, EmitterWriteResult};
use nervix_connector_mongodb::{MongoDbClient, MongoDbClientProvider};
use nervix_connector_mysql::{MySqlConnection, MySqlConnections};
use nervix_connector_postgres::{PostgresConnection, PostgresConnections};
use nervix_connector_redis::{RedisCommandPool, RedisPoolHandle, RedisPoolServices, RedisPoolWait};

use super::*;
use crate::runtime::shared_clients::{PoolWaitRegistration, PoolWaitSlot};

/// One emitter's interest in this node's shared pool for its client.
///
/// The pool belongs to the named client, so every local emitter of that client borrows from it and the
/// declared maximum bounds the node's connections rather than this emitter's.
pub(super) struct PooledEmitterClient {
    lease: SharedClientLease,
    /// The client borrowed from, named in this emitter's diagnostics and in its pool wait.
    client: ClientName,
    wait: PoolWaitRegistration,
}

impl PooledEmitterClient {
    /// Take this emitter's interest in the node's instance of `client`, opening it on first local use.
    pub(super) async fn lease(
        context: &EmitterConnectorContext,
        client: &EmitterClientSpec,
        pool: PooledClientPlan,
    ) -> Result<Self, Report<SharedClientError>> {
        let lease = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "emitter initialization installs and retains one shared-client lease before \
             publishing records",
            context
                .runtime
                .lease_shared_client(&context.domain, client, pool)
        )
        .await?;
        Ok(Self {
            lease,
            client: client.name.clone(),
            wait: nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "emitter initialization installs and retains its one pool-wait publication before \
                 borrowing connections",
                context.runtime.register_pool_wait(
                    DomainNodeRef::node_in(
                        context.domain.clone(),
                        ModelKind::Emitter,
                        context.emitter.clone(),
                    ),
                    client.name.clone(),
                )
            ),
        })
    }

    fn not_initialized(connector: &'static str) -> EmitterWriteError {
        EmitterWriteError::NotInitialized { connector }
    }
}

#[async_trait]
impl MySqlConnections for PooledEmitterClient {
    /// Borrow a connection for one insert, reporting the wait until the pool hands one over.
    ///
    /// The borrow covers one bounded insert and its row-by-row isolation when the database refuses
    /// a record. It returns to the shared pool before the next insert, flush interval or retry
    /// backoff.
    async fn connection(&self) -> EmitterWriteResult<MySqlConnection> {
        let pool = self
            .lease
            .client()
            .mysql(&self.client)
            .map_err(|error| error.change_context(Self::not_initialized("mysql")))?;
        PoolWaitSlot::borrow(&self.wait.slot, pool.connection()).await
    }
}

#[async_trait]
impl PostgresConnections for PooledEmitterClient {
    /// Borrow a connection for one insert, reporting the wait until the pool hands one over.
    ///
    /// The borrow covers one bounded insert, together with the write's metadata lookup on its first
    /// insert and the isolation of the rows of an insert the database refused, and no more: between
    /// inserts, and across a flush interval or a retry backoff, this emitter holds no connection.
    async fn connection(&self) -> EmitterWriteResult<PostgresConnection> {
        let pool = self
            .lease
            .client()
            .postgres(&self.client)
            .map_err(|error| error.change_context(Self::not_initialized("postgres")))?;
        PoolWaitSlot::borrow(&self.wait.slot, pool.connection()).await
    }
}

impl MongoDbClientProvider for PooledEmitterClient {
    /// The shared driver client this emitter writes through, which pools its own connections per
    /// server in the topology and is therefore never borrowed per write.
    fn client(&self) -> EmitterWriteResult<MongoDbClient> {
        self.lease
            .client()
            .mongodb(&self.client)
            .cloned()
            .map_err(|error| error.change_context(Self::not_initialized("mongodb")))
    }
}

impl EmitterConnectorContext {
    /// Leases this node's shared Redis command pool for `plan`'s client.
    ///
    /// The lease travels with the handle the emitter holds, so the pool stays open for exactly as
    /// long as the emitter publishes through it.
    pub(super) async fn lease_redis_pool(
        &self,
        plan: &RedisPubSubEmitterPlan,
    ) -> EmitterRuntimeResult<RedisPoolHandle> {
        let lease = nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "emitter initialization installs and retains one shared-client lease before \
             publishing records",
            self.runtime
                .lease_shared_client(&self.domain, &plan.client, plan.pooled_client())
        )
        .await
        .map_err(|error| emitter_init_error(error.to_string()))?;
        let pool = lease
            .client()
            .redis(&plan.client.name)
            .map_err(|error| emitter_init_error(error.to_string()))?
            .clone();
        Ok(RedisPoolHandle::new(LeasedRedisPool {
            _lease: lease,
            pool,
            wait: nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "emitter initialization installs and retains its one pool-wait publication before \
                 borrowing connections",
                self.runtime.register_pool_wait(
                    DomainNodeRef::node_in(
                        self.domain.clone(),
                        ModelKind::Emitter,
                        self.emitter.clone(),
                    ),
                    plan.client.name.clone(),
                )
            ),
        }))
    }
}

/// One emitter's interest in this node's shared Redis pool, and the wait it records while that
/// pool hands a connection over.
struct LeasedRedisPool {
    /// Held for as long as the emitter publishes: releasing it gives back this emitter's interest in
    /// the shared client, which closes the pool once its last local user leaves.
    _lease: SharedClientLease,
    pool: RedisCommandPool,
    wait: PoolWaitRegistration,
}

impl RedisPoolServices for LeasedRedisPool {
    fn pool(&self) -> RedisCommandPool {
        self.pool.clone()
    }

    fn begin_pool_wait(&self) -> RedisPoolWait {
        RedisPoolWait::new(PoolWaitSlot::begin(&self.wait.slot))
    }
}
