//! Reqwest's two resolved DNS hooks backed by the node's configured resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Adapting one node resolver to both Reqwest dependency versions.
//! - **Depends on.** The node resolver and Reqwest's public DNS traits.
//! - **Must not know.** Connector configuration, HTTP requests, Iceberg tables, or retry policy.

use std::{error::Error, net::SocketAddr};

use crate::DnsResolver;

type ResolveError = Box<dyn Error + Send + Sync>;
type Addresses = Box<dyn Iterator<Item = SocketAddr> + Send>;

impl DnsResolver {
    async fn reqwest_addresses(&self, name: String) -> Result<Addresses, ResolveError> {
        let addresses = self
            .hook_addresses(&name)
            .await
            .map_err(|report| -> ResolveError { Box::new(report.current_context().clone()) })?;
        let mut sockets = Vec::with_capacity(addresses.len());
        for ip in addresses {
            // Reqwest gives an address whose port is zero the URL's port, or its scheme's default.
            sockets.push(SocketAddr::new(ip, 0));
        }
        Ok(Box::new(sockets.into_iter()))
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
