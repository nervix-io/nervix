//! Outside the layer order: the Postgres clients used to provision and observe test data.
//!
//! - **Owns.** One shared plaintext pool and one shared certificate-verified TLS pool per environment,
//!   the connections they establish once and keep, the separate connections table-lock controllers
//!   open, and closing both pools before the database container stops.
//! - **Depends on.** The environment's published endpoints and TLS material, SQLx, and primitives.
//! - **Must not know.** Scenario admission, product client declarations, or emitter behavior.

use std::{fmt, io};

use nervix_primitives::sync::Arc;
use sqlx::{
    Connection as _,
    postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions, PgSslMode},
};

use crate::{DependencyEndpoints, POSTGRES_ADDR, POSTGRES_TLS_ADDR};

/// Short setup and observation queries share this many connections per transport. The pool
/// establishes all of them when it opens and keeps them until it closes, because establishing one
/// while scenarios run waits for the busy runtime.
const CONNECTIONS_PER_TRANSPORT: u32 = 2;

/// The application name a table-lock controller's connection reports to Postgres.
const LOCK_APPLICATION_NAME: &str = "nervix_test_lock";

/// The environment's observation clients. Cloning shares their existing pools.
#[derive(Clone)]
pub struct PostgresClients(Arc<PostgresPools>);

struct PostgresPools {
    plaintext: PgPool,
    tls: PgPool,
}

impl fmt::Debug for PostgresClients {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresClients")
            .field("plaintext_connections", &self.0.plaintext.size())
            .field("tls_connections", &self.0.tls.size())
            .finish()
    }
}

impl PostgresClients {
    pub(super) async fn open(endpoints: &DependencyEndpoints) -> io::Result<Self> {
        let plaintext = Self::connect(endpoints, false).await?;
        let tls = match Self::connect(endpoints, true).await {
            Ok(tls) => tls,
            Err(error) => {
                plaintext.close().await;
                return Err(error);
            }
        };
        Ok(Self(Arc::new(PostgresPools { plaintext, tls })))
    }

    async fn connect(endpoints: &DependencyEndpoints, tls: bool) -> io::Result<PgPool> {
        let address = if tls {
            endpoints.get(POSTGRES_TLS_ADDR)?
        } else {
            endpoints.get(POSTGRES_ADDR)?
        };
        let mut options: PgConnectOptions = address.parse().map_err(io::Error::other)?;
        options = options.application_name("nervix_test_observer");
        if tls {
            options = options
                .ssl_mode(PgSslMode::VerifyFull)
                .ssl_root_cert(endpoints.tls_ca_path()?);
        }
        Self::pool_options()
            .connect_with(options)
            .await
            .map_err(io::Error::other)
    }

    /// Every connection is established while the environment opens the pool, and no idle or
    /// lifetime retirement takes one away before the environment closes it.
    fn pool_options() -> PgPoolOptions {
        PgPoolOptions::new()
            .min_connections(CONNECTIONS_PER_TRANSPORT)
            .max_connections(CONNECTIONS_PER_TRANSPORT)
            .idle_timeout(None)
            .max_lifetime(None)
    }

    fn pool(&self, tls: bool) -> &PgPool {
        if tls { &self.0.tls } else { &self.0.plaintext }
    }

    /// A view of the selected observation pool, ready to borrow a connection.
    pub fn client(&self, tls: bool) -> PgPool {
        self.pool(tls).clone()
    }

    /// A connection of its own for a table-lock controller, with the selected transport's
    /// observation options. A held lock keeps its connection for as long as its scenario needs,
    /// so it never takes one the observation pool established.
    pub async fn lock_connection(&self, tls: bool) -> io::Result<PgConnection> {
        let options = self
            .pool(tls)
            .connect_options()
            .as_ref()
            .clone()
            .application_name(LOCK_APPLICATION_NAME);
        PgConnection::connect_with(&options)
            .await
            .map_err(io::Error::other)
    }

    pub(super) async fn close(&self) {
        self.0.plaintext.close().await;
        self.0.tls.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[nervix_primitives::test]
    async fn closing_the_owner_closes_both_transports_through_retained_views() {
        let clients = PostgresClients(Arc::new(PostgresPools {
            plaintext: PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new()),
            tls: PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new()),
        }));
        let retained = clients.clone();
        let plaintext = retained.client(false);
        let tls = retained.client(true);
        assert!(!plaintext.is_closed());
        assert!(!tls.is_closed());

        clients.close().await;

        assert!(matches!(
            plaintext.acquire().await,
            Err(sqlx::Error::PoolClosed)
        ));
        assert!(matches!(tls.acquire().await, Err(sqlx::Error::PoolClosed)));
    }

    #[test]
    fn observation_pools_establish_every_connection_up_front_and_keep_it() {
        let options = PostgresClients::pool_options();

        assert_eq!(options.get_min_connections(), CONNECTIONS_PER_TRANSPORT);
        assert_eq!(options.get_max_connections(), CONNECTIONS_PER_TRANSPORT);
        assert_eq!(options.get_idle_timeout(), None);
        assert_eq!(options.get_max_lifetime(), None);
    }
}
