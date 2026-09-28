//! Smithy's HTTP client DNS hook backed by the node's configured resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Adapting one node resolver to the `ResolveDns` trait an AWS SDK HTTP client built
//!   with `build_with_resolver` resolves a service host through.
//! - **Depends on.** The node resolver and the Smithy runtime API's DNS trait.
//! - **Must not know.** Which service or SDK client uses the connector, request signing, TLS, or
//!   retry policy.
//!
//! Smithy hands the hook to `hyper-util`'s `HttpConnector`, which dials a literal IPv4 or IPv6 host
//! without asking it and applies the URL's port to every address it answers.

use std::net::IpAddr;

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};

use crate::DnsResolver;

impl ResolveDns for DnsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(self.smithy_addresses(name))
    }
}

impl DnsResolver {
    async fn smithy_addresses(&self, name: &str) -> Result<Vec<IpAddr>, ResolveDnsError> {
        let addresses = self
            .hook_addresses(name)
            .await
            .map_err(|report| ResolveDnsError::new(report.current_context().clone()))?;
        Ok(addresses.into_iter().collect())
    }
}
