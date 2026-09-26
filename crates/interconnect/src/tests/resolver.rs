//! The identity and peer resolver every library test transport is built with.
//!
//! Layer: test harness.
//!
//! - **Owns.** A transport identity advertising `localhost`, and a resolver that answers it without a
//!   name server.
//! - **Depends on.** `nervix-dns` in production builds and the simulated resolver in the Turmoil
//!   build.
//! - **Must not know.** What the tests that use them check.

#[cfg(not(feature = "turmoil"))]
use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;
#[cfg(not(feature = "turmoil"))]
use tempfile::tempdir;

use crate::{PeerResolver, TransportIdentity};

pub(super) fn localhost_identity(cluster_id: &str, node_id: ClusterNodeName) -> TransportIdentity {
    TransportIdentity {
        cluster_id: cluster_id.to_string(),
        node_id,
        advertised_host: "localhost".to_string(),
    }
}

/// A resolver that answers `localhost` from its own hosts file and asks no name server.
#[cfg(not(feature = "turmoil"))]
pub(super) async fn test_resolver() -> PeerResolver {
    let files = tempdir().assured("a temporary directory can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(&resolver_configuration, "nameserver 192.0.2.53\n")
        .assured("the test resolver configuration can be written");
    std::fs::write(&hosts_file, "127.0.0.1 localhost\n")
        .assured("the test hosts file can be written");
    let dns = nervix_dns::DnsResolver::load(nervix_dns::DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: nervix_dns::NameServers::ResolverConfiguration,
    })
    .await
    .assured("the test resolver configuration is valid");
    PeerResolver::new(dns)
}

/// The simulated host's resolver.
#[cfg(feature = "turmoil")]
pub(super) async fn test_resolver() -> PeerResolver {
    PeerResolver::simulated()
}
