//! Asynchronous host name resolution for the connections a node opens.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** One node's resolver: reading the resolver configuration and hosts file it is given,
//!   the answer cache and its TTL bounds, the bound on concurrent lookups, and the typed outcome of
//!   every lookup.
//! - **Depends on.** Hickory's Tokio resolver and the `resolv.conf` grammar.
//! - **Must not know.** Models, peers, connectors, graphs, or any protocol's retry policy.
//!
//! # Resolution order
//!
//! A literal IPv4 or IPv6 address, bracketed or not, is its own answer and sends no query. Any other
//! host is looked up in the hosts file exactly as written, and an entry there answers alone: no DNS
//! query is sent for it, for either address family. Everything else goes to the configured name
//! servers, completed by the search list according to `ndots`, asking for IPv4 and IPv6 addresses
//! together and answering IPv4 addresses first. A name with a trailing dot is fully qualified and
//! is queried exactly as written.
//!
//! # Bounds
//!
//! Every lookup runs within the budget its caller gives it, so name resolution never outlasts the
//! connection or request deadline it belongs to. At most [`MAX_CONCURRENT_LOOKUPS`] DNS lookups run
//! at once; a lookup beyond that waits for a slot inside its own budget. Answers are cached for
//! their DNS TTL, bounded above by one hour for addresses and thirty seconds for a name that does not
//! exist or has no address, in a cache of 4,096 entries. An expired answer is looked up again on the
//! next resolution. The host configuration's `timeout` and `attempts` bound each query to one name
//! server; the caller's budget bounds the lookup as a whole.

use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use error_stack::Report;
use hickory_resolver::{
    TokioResolver,
    net::{NetError, runtime::TokioRuntimeProvider},
    proto::rr::Name,
};
use indexmap::IndexSet;
use meticulous::ResultExt as _;
use tokio::{sync::Semaphore, time::timeout};
use triomphe::Arc;

mod configuration;
mod hosts;
mod lookup;

pub use configuration::{
    DnsConfiguration, DnsConfigurationError, NameServers, SYSTEM_HOSTS_FILE,
    SYSTEM_RESOLVER_CONFIGURATION,
};
pub use lookup::{DnsLookupError, DnsLookupFailure};

use crate::{configuration::LoadedConfiguration, hosts::HostsTable};

/// The most DNS lookups one resolver has in flight at once. A policy input: enough for every peer
/// of a full cluster topology to reconnect at the same time, small enough that a silent name server
/// cannot collect an unbounded number of waiting lookups.
pub const MAX_CONCURRENT_LOOKUPS: usize = 64;

/// One node's resolver. Clones share the configuration, the hosts file, the answer cache, and the
/// concurrency bound; the resolver's background tasks end when the last clone is dropped.
#[derive(Clone)]
pub struct DnsResolver {
    inner: Arc<ResolverState>,
}

struct ResolverState {
    dns: TokioResolver,
    hosts: HostsTable,
    lookups: Semaphore,
}

impl DnsResolver {
    /// Read `configuration` and construct the resolver it describes.
    ///
    /// The configuration and hosts files are read once, on the blocking pool, so a slow filesystem
    /// never stalls the caller's reactor. Neither file is read again: a changed file takes effect
    /// when the node next starts.
    pub async fn load(
        configuration: DnsConfiguration,
    ) -> Result<Self, Report<DnsConfigurationError>> {
        let reading = tokio::task::spawn_blocking(move || configuration.read()).await;
        let loaded = match reading {
            Ok(loaded) => loaded?,
            Err(error) => {
                return Err(Report::new(DnsConfigurationError::Interrupted).attach_printable(error));
            }
        };
        let LoadedConfiguration {
            resolver,
            options,
            hosts,
        } = loaded;
        let mut builder =
            TokioResolver::builder_with_config(resolver, TokioRuntimeProvider::default());
        *builder.options_mut() = options;
        let dns = builder
            .build()
            .map_err(|error| Report::new(DnsConfigurationError::Build).attach_printable(error))?;
        Ok(Self {
            inner: Arc::new(ResolverState {
                dns,
                hosts,
                lookups: Semaphore::new(MAX_CONCURRENT_LOOKUPS),
            }),
        })
    }

    /// Every address `host` resolves to now, each paired with `port`, in resolution order.
    ///
    /// The lookup ends within `budget`, including any wait for a lookup slot. A successful result
    /// holds at least one address.
    pub async fn resolve(
        &self,
        host: &str,
        port: u16,
        budget: Duration,
    ) -> Result<Vec<SocketAddr>, Report<DnsLookupError>> {
        let addresses = self.resolve_addresses(host, budget).await?;
        let mut sockets = Vec::with_capacity(addresses.len());
        for ip in addresses {
            sockets.push(SocketAddr::new(ip, port));
        }
        Ok(sockets)
    }

    async fn resolve_addresses(
        &self,
        host: &str,
        budget: Duration,
    ) -> Result<IndexSet<IpAddr>, Report<DnsLookupError>> {
        let written = unbracketed(host);
        if let Ok(ip) = written.parse::<IpAddr>() {
            return Ok(IndexSet::from([ip]));
        }
        let name = match Name::from_utf8(written) {
            Ok(name) => name,
            Err(error) => {
                let failure = DnsLookupError::new(host, DnsLookupFailure::InvalidName);
                return Err(Report::new(failure).attach_printable(error));
            }
        };
        if let Some(addresses) = self.inner.hosts.addresses(&name) {
            return Ok(addresses);
        }
        let Ok(queried) = timeout(budget, self.query(name)).await else {
            let failure = DnsLookupError::new(host, DnsLookupFailure::Timeout);
            return Err(Report::new(failure).attach_printable(format!("budget {budget:?}")));
        };
        let addresses = match queried {
            Ok(addresses) => addresses,
            Err(error) => {
                let failure = DnsLookupError::new(host, DnsLookupFailure::of(&error));
                return Err(Report::new(failure).attach_printable(error));
            }
        };
        if addresses.is_empty() {
            let failure = DnsLookupError::new(host, DnsLookupFailure::NoAddresses);
            return Err(Report::new(failure));
        }
        Ok(addresses)
    }

    /// Ask the name servers for `name` once a lookup slot is free.
    async fn query(&self, name: Name) -> Result<IndexSet<IpAddr>, NetError> {
        let _slot = self
            .inner
            .lookups
            .acquire()
            .await
            .assured("the resolver never closes its lookup semaphore");
        let lookup = self.inner.dns.lookup_ip(name).await?;
        let mut addresses = IndexSet::new();
        for ip in lookup.iter() {
            addresses.insert(ip);
        }
        Ok(addresses)
    }
}

impl fmt::Debug for DnsResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DnsResolver")
    }
}

/// `host` without the brackets that enclose a literal IPv6 address in an authority.
fn unbracketed(host: &str) -> &str {
    let Some(inner) = host.strip_prefix('[') else {
        return host;
    };
    let Some(inner) = inner.strip_suffix(']') else {
        return host;
    };
    inner
}
