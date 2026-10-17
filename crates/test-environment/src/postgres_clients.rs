//! Outside the layer order: the Postgres clients used to provision and observe test data.
//!
//! - **Owns.** One shared plaintext pool and one shared certificate-verified TLS pool per environment,
//!   their connection bounds, and their closure before the database container stops.
//! - **Depends on.** The environment's published endpoints and TLS material, SQLx, and primitives.
//! - **Must not know.** Scenario admission, product client declarations, or emitter behavior.

use std::{fmt, io};

use nervix_primitives::sync::Arc;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};

use crate::{DependencyEndpoints, POSTGRES_ADDR, POSTGRES_TLS_ADDR};

/// Short setup and observation queries share a bounded pool; lock controllers detach their socket.
const CONNECTIONS_PER_TRANSPORT: u32 = 2;

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
        PgPoolOptions::new()
            .max_connections(CONNECTIONS_PER_TRANSPORT)
            .connect_with(options)
            .await
            .map_err(io::Error::other)
    }

    /// A view of the selected observation pool, ready to borrow a connection.
    pub fn client(&self, tls: bool) -> PgPool {
        if tls {
            self.0.tls.clone()
        } else {
            self.0.plaintext.clone()
        }
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
}
