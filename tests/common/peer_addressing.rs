//! How a test cluster's nodes address one another on the interconnect, and the DNS they resolve
//! those addresses through.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The addressing a scenario selects, the interconnect listen address and advertised
//!   host of each node under it, the scenario's DNS authority and zone, and the resolver
//!   configuration and hosts file the nodes load.
//! - **Depends on.** The in-process DNS authority from the test environment and the resolver
//!   configuration type the server loads.
//! - **Must not know.** Cluster startup, node lifecycle, or scenario steps.
//!
//! # Names and addresses
//!
//! A named node listens on its own loopback address, `127.0.2.<n>` for node `node-<n>`, and a node
//! that moves listens on `127.0.3.<n>` instead. Nothing listens on `127.0.4.<n>`, which is the
//! address a zone lists first when a scenario needs an answer that cannot connect. Every answer
//! carries a one-second TTL, so a scenario that changes the zone observes the change within seconds.
//! A literal addressing needs no name resolution, and its nodes load the host's own resolver
//! configuration as a node in production does.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    time::Duration,
};

use meticulous::OptionExt as _;
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use strum::EnumString;

use crate::common::port_pool::{next_ports, release_test_ports};

/// The domain every fixture name belongs to and the resolver's search domain.
pub(crate) const FIXTURE_DOMAIN: &str = "nervix.test";
/// The TTL of every fixture answer, positive or negative.
const FIXTURE_TTL: Duration = Duration::from_secs(1);
/// One-second queries with one retry, completed by the fixture domain.
const FIXTURE_RESOLVER_CONFIGURATION: &str =
    "search nervix.test\noptions ndots:1 timeout:1 attempts:2\n";

/// How the nodes of a test cluster address one another on the interconnect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, EnumString)]
pub(crate) enum PeerAddressing {
    /// Literal IPv4 loopback endpoints.
    #[default]
    #[strum(serialize = "literal IPv4 endpoints")]
    LiteralIpv4,
    /// Literal IPv6 loopback endpoints.
    #[strum(serialize = "literal IPv6 endpoints")]
    LiteralIpv6,
    /// Fully qualified names the scenario's DNS authority answers.
    #[strum(serialize = "DNS names")]
    DnsNames,
    /// Fully qualified names whose first answer is an address nothing listens on.
    #[strum(serialize = "DNS names behind an unreachable address")]
    DnsNamesBehindUnreachableAddress,
    /// Single-label names the resolver completes with the fixture domain.
    #[strum(serialize = "single-label DNS names")]
    SingleLabelDnsNames,
    /// Fully qualified names the hosts file lists, while the DNS authority never answers them.
    #[strum(serialize = "hosts file names")]
    HostsFileNames,
}

/// Where one node's interconnect listens, and the host it advertises for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InterconnectAddress {
    pub(crate) listen_ip: IpAddr,
    pub(crate) advertised_host: String,
}

impl PeerAddressing {
    /// Whether nodes resolve one another through the scenario's DNS fixture.
    fn resolves_names(self) -> bool {
        match self {
            Self::LiteralIpv4 | Self::LiteralIpv6 => false,
            Self::DnsNames
            | Self::DnsNamesBehindUnreachableAddress
            | Self::SingleLabelDnsNames
            | Self::HostsFileNames => true,
        }
    }

    /// The interconnect address of node `node-<index>`.
    pub(crate) fn address(self, node_id: &str, index: u8) -> InterconnectAddress {
        let listen_ip = match self {
            Self::LiteralIpv4 => IpAddr::V4(Ipv4Addr::LOCALHOST),
            Self::LiteralIpv6 => IpAddr::V6(Ipv6Addr::LOCALHOST),
            Self::DnsNames
            | Self::DnsNamesBehindUnreachableAddress
            | Self::SingleLabelDnsNames
            | Self::HostsFileNames => IpAddr::V4(Ipv4Addr::new(127, 0, 2, index)),
        };
        let advertised_host = match self {
            Self::LiteralIpv4 | Self::LiteralIpv6 => listen_ip.to_string(),
            Self::DnsNames | Self::DnsNamesBehindUnreachableAddress | Self::HostsFileNames => {
                qualified_name(node_id)
            }
            Self::SingleLabelDnsNames => node_id.to_string(),
        };
        InterconnectAddress {
            listen_ip,
            advertised_host,
        }
    }

    /// The interconnect address of node `node-<index>` after it moves: the same advertised host on
    /// another loopback address.
    pub(crate) fn moved_address(self, node_id: &str, index: u8) -> InterconnectAddress {
        let InterconnectAddress {
            advertised_host, ..
        } = self.address(node_id, index);
        InterconnectAddress {
            listen_ip: IpAddr::V4(Ipv4Addr::new(127, 0, 3, index)),
            advertised_host,
        }
    }

    fn unreachable_ip(index: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(127, 0, 4, index))
    }
}

/// The name the fixture zone answers for `node_id`.
pub(crate) fn qualified_name(node_id: &str) -> String {
    format!("{node_id}.{FIXTURE_DOMAIN}")
}

/// The DNS a named cluster resolves through: one authority, its zone, and the files nodes load.
///
/// The authority's UDP port is drawn from the harness port pool, like every other fixture port, and
/// goes back when the cluster is dropped at the end of cleanup, once no node can still ask it.
#[derive(Debug)]
pub(crate) struct ClusterDns {
    addressing: PeerAddressing,
    authority: DnsAuthority,
    port: u16,
    configuration: DnsConfiguration,
}

/// One node the fixture publishes, by name and listen address.
pub(crate) struct PublishedNode<'a> {
    pub(crate) node_id: &'a str,
    pub(crate) index: u8,
    pub(crate) listen_ip: IpAddr,
}

impl ClusterDns {
    /// Start the DNS a cluster addressed by `addressing` resolves through, publishing `nodes`, and
    /// write its resolver configuration and hosts file into `directory`. Literal addressing
    /// resolves nothing, so it starts no fixture.
    pub(crate) async fn start(
        addressing: PeerAddressing,
        directory: &Path,
        nodes: &[PublishedNode<'_>],
    ) -> io::Result<Option<Self>> {
        if !addressing.resolves_names() {
            return Ok(None);
        }
        let port = next_ports(1)?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("the port pool returned no port"))?;
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let authority = match DnsAuthority::start(bind).await {
            Ok(authority) => authority,
            Err(error) => {
                release_test_ports(&[port]);
                return Err(error);
            }
        };
        let configuration = DnsConfiguration {
            resolver_configuration: directory.join("resolv.conf"),
            hosts_file: directory.join("hosts"),
            name_servers: NameServers::Explicit(vec![authority.address()]),
        };
        // Constructed before the files are written, so a failed write still returns the port.
        let dns = Self {
            addressing,
            authority,
            port,
            configuration,
        };
        let mut hosts = String::from("127.0.0.1 localhost\n::1 localhost\n");
        if addressing == PeerAddressing::HostsFileNames {
            for node in nodes {
                hosts.push_str(&format!(
                    "{} {}\n",
                    node.listen_ip,
                    qualified_name(node.node_id)
                ));
            }
        }
        std::fs::write(
            &dns.configuration.resolver_configuration,
            FIXTURE_RESOLVER_CONFIGURATION,
        )?;
        std::fs::write(&dns.configuration.hosts_file, hosts)?;
        for node in nodes {
            dns.publish(node.node_id, node.index, node.listen_ip);
        }
        Ok(Some(dns))
    }

    /// The resolver configuration every node of the cluster loads.
    pub(crate) fn configuration(&self) -> DnsConfiguration {
        self.configuration.clone()
    }

    /// Answer `node_id`'s name with `listen_ip`, behind an address that cannot connect when the
    /// addressing asks for one. A hosts-file cluster leaves the name silent, so any question for
    /// it would stall rather than answer.
    pub(crate) fn publish(&self, node_id: &str, index: u8, listen_ip: IpAddr) {
        let name = qualified_name(node_id);
        let answer = match self.addressing {
            PeerAddressing::HostsFileNames => DnsAnswer::Silent,
            PeerAddressing::DnsNamesBehindUnreachableAddress => DnsAnswer::Addresses {
                addresses: vec![PeerAddressing::unreachable_ip(index), listen_ip],
                ttl: FIXTURE_TTL,
            },
            PeerAddressing::LiteralIpv4
            | PeerAddressing::LiteralIpv6
            | PeerAddressing::DnsNames
            | PeerAddressing::SingleLabelDnsNames => DnsAnswer::Addresses {
                addresses: vec![listen_ip],
                ttl: FIXTURE_TTL,
            },
        };
        self.authority.set(&name, answer);
    }

    /// Answer `node_id`'s name with `answer` from the next question on.
    pub(crate) fn answer(&self, node_id: &str, answer: FixtureAnswer) {
        let answer = match answer {
            FixtureAnswer::NameNotFound => DnsAnswer::NameNotFound {
                negative_ttl: FIXTURE_TTL,
            },
            FixtureAnswer::NoAddresses => DnsAnswer::NoAddresses {
                negative_ttl: FIXTURE_TTL,
            },
            FixtureAnswer::Silence => DnsAnswer::Silent,
        };
        self.authority.set(&qualified_name(node_id), answer);
    }

    /// Questions the authority received for the names of `node_ids`.
    pub(crate) fn questions_for(&self, node_ids: &[String]) -> u64 {
        let mut questions = 0_u64;
        for node_id in node_ids {
            let asked = self.authority.questions_for(&qualified_name(node_id));
            questions = questions
                .checked_add(asked)
                .assured("a scenario cannot ask 2^64 questions");
        }
        questions
    }
}

impl Drop for ClusterDns {
    fn drop(&mut self) {
        release_test_ports(&[self.port]);
    }
}

/// An answer a scenario gives a node's name in place of its address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
pub(crate) enum FixtureAnswer {
    #[strum(serialize = "name not found")]
    NameNotFound,
    #[strum(serialize = "no addresses")]
    NoAddresses,
    #[strum(serialize = "silence")]
    Silence,
}
