//! The node's pooled clients, as the connectors that borrow from them see them.
//!
//! Layer: data plane.
//! - **Owns.** The lease one pooled sink holds on its client's node-wide instance, the wait it
//!   records while it has asked for a connection and not been handed one, and the connection
//!   sources its connector calls.
//! - **Depends on.** The shared client registry, the vocabulary that names a domain, a client and
//!   a graph node, and the pooled connector crates.
//! - **Must not know.** How a batch is mapped, how a row is encoded, or what a sink writes.

use async_trait::async_trait;
use nervix_connector::{SinkPublishError, SinkPublishResult};
use nervix_connector_mongodb::{MongoDbClient, MongoDbClientSource};
use nervix_connector_mysql::{MySqlConnection, MySqlConnections};
use nervix_connector_postgres::{PostgresConnection, PostgresConnections};
use nervix_connector_redis::{RedisCommandPool, RedisPoolHandle, RedisPoolServices, RedisPoolWait};

use super::*;
use crate::runtime::shared_clients::PoolWaitGuard;

/// One sink's interest in this node's shared pool for its client.
///
/// The pool belongs to the named client, so every local sink of that client borrows from it and the
/// declared maximum bounds the node's connections rather than this sink's.
pub(super) struct PooledSinkClient {
    lease: SharedClientLease,
    /// The client borrowed from, named in this sink's diagnostics and in its pool wait.
    client: ClientName,
    runtime: Runtime,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl PooledSinkClient {
    /// Take this sink's interest in the node's instance of `client`, opening it on first local use.
    pub(super) async fn lease(
        context: &EmitterSinkContext,
        client: &EmitterClientSpec,
        pool: PooledClientPlan,
    ) -> Result<Self, Report<SharedClientError>> {
        let lease = context
            .runtime
            .lease_shared_client(&context.domain, client, pool)
            .await?;
        Ok(Self {
            lease,
            client: client.name.clone(),
            runtime: context.runtime.clone(),
            waiter: DomainNodeRef::node_in(
                context.domain.clone(),
                ModelKind::Emitter,
                context.emitter.clone(),
            ),
        })
    }

    /// Marks this sink as waiting for a connection until the returned guard is dropped.
    fn wait_guard(&self) -> PoolWaitGuard {
        self.runtime.pool_wait_guard(&self.waiter, &self.client)
    }

    fn not_initialized(sink: &'static str) -> SinkPublishError {
        SinkPublishError::NotInitialized { sink }
    }
}

#[async_trait]
impl MySqlConnections for PooledSinkClient {
    /// Borrow a connection for one insert, reporting the wait until the pool hands one over.
    ///
    /// The borrow lasts for the insert and no longer: the connection returns to the shared pool
    /// when the borrow is dropped, so a flush between inserts holds none.
    async fn connection(&self) -> SinkPublishResult<MySqlConnection> {
        let pool = self
            .lease
            .client()
            .mysql(&self.client)
            .map_err(|error| error.change_context(Self::not_initialized("mysql")))?;
        let waiting = self.wait_guard();
        let connection = pool.connection().await;
        drop(waiting);
        connection
    }
}

#[async_trait]
impl PostgresConnections for PooledSinkClient {
    /// Borrow a connection for one operation, reporting the wait until the pool hands one over.
    ///
    /// The borrow covers a bounded insert or its metadata work and no more: between inserts, and
    /// across a flush interval or a retry backoff, this sink holds no connection.
    async fn connection(&self) -> SinkPublishResult<PostgresConnection> {
        let pool = self
            .lease
            .client()
            .postgres(&self.client)
            .map_err(|error| error.change_context(Self::not_initialized("postgres")))?;
        let waiting = self.wait_guard();
        let connection = pool.connection().await;
        drop(waiting);
        connection
    }
}

impl MongoDbClientSource for PooledSinkClient {
    /// The shared driver client this sink writes through, which pools its own connections per
    /// server in the topology and is therefore never borrowed per write.
    fn client(&self) -> SinkPublishResult<MongoDbClient> {
        self.lease
            .client()
            .mongodb(&self.client)
            .cloned()
            .map_err(|error| error.change_context(Self::not_initialized("mongodb")))
    }
}

impl EmitterSinkContext {
    /// Leases this node's shared Redis command pool for `plan`'s client.
    ///
    /// The lease travels with the handle the sink holds, so the pool stays open for exactly as
    /// long as the sink publishes through it.
    pub(super) async fn lease_redis_pool(
        &self,
        plan: &RedisSinkPlan,
    ) -> EmitterRuntimeResult<RedisPoolHandle> {
        let lease = self
            .runtime
            .lease_shared_client(&self.domain, &plan.client, plan.pooled_client())
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
            runtime: self.runtime.clone(),
            client: plan.client.name.clone(),
            waiter: DomainNodeRef::node_in(
                self.domain.clone(),
                ModelKind::Emitter,
                self.emitter.clone(),
            ),
        }))
    }
}

/// One emitter's interest in this node's shared Redis pool, and the wait it records while that
/// pool hands a connection over.
struct LeasedRedisPool {
    /// Held for as long as the sink publishes: releasing it gives back this emitter's interest in
    /// the shared client, which closes the pool once its last local user leaves.
    _lease: SharedClientLease,
    pool: RedisCommandPool,
    runtime: Runtime,
    /// The client borrowed from, named in this emitter's pool wait.
    client: ClientName,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl RedisPoolServices for LeasedRedisPool {
    fn pool(&self) -> RedisCommandPool {
        self.pool.clone()
    }

    fn begin_pool_wait(&self) -> RedisPoolWait {
        RedisPoolWait::new(self.runtime.pool_wait_guard(&self.waiter, &self.client))
    }
}
