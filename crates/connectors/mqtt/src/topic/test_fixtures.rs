//! A local DNS authority and the node resolver that asks it, for the MQTT connector's checks.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** Temporary resolver files and one loopback DNS authority.
//! - **Depends on.** The DNS engine and the test environment's DNS authority.
//! - **Must not know.** The server's graph, the MQTT broker, or the runtime host.

use std::{net::IpAddr, time::Duration};

use meticulous::ResultExt as _;
use nervix_dns::{DnsConfiguration, DnsResolver, NameServers};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;

/// A resolver asking one local authority, with the files it was loaded from. Its configuration has
/// no search list, so a silent authority ends a lookup as a timeout.
pub(crate) struct DnsFixture {
    pub(crate) authority: DnsAuthority,
    pub(crate) dns: DnsResolver,
    _files: TempDir,
}

impl DnsFixture {
    pub(crate) async fn start() -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback UDP port is available");
        let files = tempfile::tempdir().assured("a temporary directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search --\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .assured("the resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the hosts file can be written");
        let dns = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .assured("the fixture configuration is valid");
        Self {
            authority,
            dns,
            _files: files,
        }
    }

    /// Answer `name` with `addresses`, in that order, for one second.
    pub(crate) fn answer(&self, name: &str, addresses: Vec<IpAddr>) {
        self.authority.set(
            name,
            DnsAnswer::Addresses {
                addresses,
                ttl: Duration::from_secs(1),
            },
        );
    }

    /// Answer that `name` does not exist, for one second.
    pub(crate) fn deny(&self, name: &str) {
        self.authority.set(
            name,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
    }
}
