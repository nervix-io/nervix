//! A local DNS authority and resolver for the Redis connector's connection tests.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** Temporary resolver files and one loopback DNS authority.
//! - **Depends on.** The DNS engine and test environment.
//! - **Must not know.** The server's graph, Redis broker, or runtime host.

use std::{fmt::Display, net::IpAddr, time::Duration};

use nervix_dns::{DnsConfiguration, DnsResolver, NameServers};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;

pub(crate) struct Fixture {
    pub(crate) authority: DnsAuthority,
    pub(crate) dns: DnsResolver,
    _files: TempDir,
}

impl Fixture {
    pub(crate) async fn start() -> Self {
        let authority = required(
            DnsAuthority::start_on_loopback().await,
            "a loopback DNS port is available",
        );
        let files = required(
            tempfile::tempdir(),
            "temporary resolver files can be created",
        );
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        required(
            std::fs::write(
                &resolver_configuration,
                "options ndots:1 timeout:1 attempts:1\n",
            ),
            "the resolver configuration can be written",
        );
        required(
            std::fs::write(&hosts_file, ""),
            "the hosts file can be written",
        );
        let dns = required(
            DnsResolver::load(DnsConfiguration {
                resolver_configuration,
                hosts_file,
                name_servers: NameServers::Explicit(vec![authority.address()]),
            })
            .await,
            "the DNS fixture configuration loads",
        );
        Self {
            authority,
            dns,
            _files: files,
        }
    }

    pub(crate) fn answer(&self, name: &str, addresses: Vec<IpAddr>) {
        self.authority.set(
            name,
            DnsAnswer::Addresses {
                addresses,
                ttl: Duration::from_secs(1),
            },
        );
    }
}

fn required<T, E: Display>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error}"),
    }
}
