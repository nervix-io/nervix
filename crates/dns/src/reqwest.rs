//! Reqwest's two resolved DNS hooks backed by the node's configured resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Adapting one node resolver to both Reqwest dependency versions.
//! - **Depends on.** The node resolver and Reqwest's public DNS traits.
//! - **Must not know.** Connector configuration, HTTP requests, Iceberg tables, or retry policy.

use std::{error::Error, net::SocketAddr, time::Duration};

use crate::DnsResolver;

type ResolveError = Box<dyn Error + Send + Sync>;
type Addresses = Box<dyn Iterator<Item = SocketAddr> + Send>;

/// An HTTP request's own timeout can cancel this lookup sooner. The bound also keeps clients
/// without a configured request timeout from waiting indefinitely on a silent name server.
const LOOKUP_BUDGET: Duration = Duration::from_secs(30);

impl DnsResolver {
    async fn reqwest_addresses(&self, name: String) -> Result<Addresses, ResolveError> {
        let addresses = self
            .resolve(&name, 0, LOOKUP_BUDGET)
            .await
            .map_err(|report| -> ResolveError { Box::new(report.current_context().clone()) })?;
        Ok(Box::new(addresses.into_iter()))
    }
}

impl reqwest::dns::Resolve for DnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let resolver = self.clone();
        let name = name.as_str().to_string();
        Box::pin(async move { resolver.reqwest_addresses(name).await })
    }
}

impl reqwest_iceberg::dns::Resolve for DnsResolver {
    fn resolve(&self, name: reqwest_iceberg::dns::Name) -> reqwest_iceberg::dns::Resolving {
        let resolver = self.clone();
        let name = name.as_str().to_string();
        Box::pin(async move { resolver.reqwest_addresses(name).await })
    }
}
