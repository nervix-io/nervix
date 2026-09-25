//! Outside the layer order: the Redis client used by the scenario harness.
//!
//! - **Owns.** Connection and response budgets for harness Redis requests.
//! - **Depends on.** The Redis driver and standard I/O errors.
//! - **Must not know.** Product connector configuration or scenario state.

use std::{io, time::Duration};

/// Allow parallel scenario scheduling delays while keeping requests finite.
pub(crate) const REDIS_REQUEST_BUDGET: Duration = Duration::from_secs(10);

pub(crate) struct TestRedisClient {
    client: redis::Client,
}

impl TestRedisClient {
    pub(crate) fn open(addr: &str) -> io::Result<Self> {
        let client = redis::Client::open(addr).map_err(io::Error::other)?;
        Ok(Self { client })
    }

    pub(crate) async fn connect(&self) -> io::Result<redis::aio::MultiplexedConnection> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(REDIS_REQUEST_BUDGET))
            .set_response_timeout(Some(REDIS_REQUEST_BUDGET));
        self.client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(io::Error::other)
    }
}
