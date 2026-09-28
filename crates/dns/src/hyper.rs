//! Hyper's connector DNS hook backed by the node's configured resolver.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Adapting one node resolver to the resolver service `hyper-util`'s `HttpConnector`
//!   resolves a URL host through.
//! - **Depends on.** The node resolver, `hyper-util`'s DNS name, and the Tower service trait.
//! - **Must not know.** Which client builds the connector, TLS, HTTP requests, or retry policy.
//!
//! The connector dials a literal IPv4 or IPv6 host without asking this service, and applies the
//! URL's port, or its scheme's default, to every address the service answers.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    vec,
};

use error_stack::Report;
use hyper_util::client::legacy::connect::dns::Name;
use tower_service::Service;

use crate::{DnsLookupError, DnsResolver};

type Addresses = vec::IntoIter<SocketAddr>;
type Resolving = Pin<Box<dyn Future<Output = Result<Addresses, DnsLookupError>> + Send>>;

impl Service<Name> for DnsResolver {
    type Response = Addresses;
    type Error = DnsLookupError;
    type Future = Resolving;

    /// A lookup waits for a slot inside its own budget, so the resolver always takes another.
    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let resolver = self.clone();
        Box::pin(async move {
            resolver
                .hyper_addresses(name)
                .await
                .map_err(|report| report.current_context().clone())
        })
    }
}

impl DnsResolver {
    async fn hyper_addresses(&self, name: Name) -> Result<Addresses, Report<DnsLookupError>> {
        let addresses = self.hook_addresses(name.as_str()).await?;
        let mut sockets = Vec::with_capacity(addresses.len());
        for ip in addresses {
            // Port zero is Hyper's own form of an address without a port: the connector gives it
            // the URL's port, or the scheme's default when the URL names none.
            sockets.push(SocketAddr::new(ip, 0));
        }
        Ok(sockets.into_iter())
    }
}
