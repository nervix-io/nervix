//! The resolver configuration and hosts file one node resolves with.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading the `resolv.conf`-format resolver configuration and the hosts file, and
//!   turning them into Hickory's configuration under this crate's cache, TTL and address-family
//!   policy.
//! - **Depends on.** The `resolv.conf` grammar and Hickory's configuration types.
//! - **Must not know.** Who resolves through the result, or what they connect to.

use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr as _,
    time::Duration,
};

use arch_into::ArchInto as _;
use error_stack::Report;
use hickory_resolver::{
    config::{
        ConnectionConfig, LookupIpStrategy, NameServerConfig, ResolveHosts, ResolverConfig,
        ResolverOpts,
    },
    proto::rr::Name,
};
use thiserror::Error;
use tracing::warn;

use crate::hosts::HostsTable;

/// The host's resolver configuration file.
pub const SYSTEM_RESOLVER_CONFIGURATION: &str = "/etc/resolv.conf";
/// The host's hosts file.
pub const SYSTEM_HOSTS_FILE: &str = "/etc/hosts";

/// Cached answers, counted per name and record type. A policy input: room for every peer of a full
/// cluster topology in both address families across a long search list, with space to spare.
const CACHE_ENTRIES: u64 = 4_096;
/// The longest an address answer is reused, whatever TTL its record carries. A policy input that
/// bounds how long a node dials an address its name no longer has.
const POSITIVE_MAX_TTL: Duration = Duration::from_secs(60 * 60);
/// The longest a missing name or an empty answer is reused. A policy input that bounds how long a
/// name that has just been published stays unresolvable.
const NEGATIVE_MAX_TTL: Duration = Duration::from_secs(30);
/// The port a `nameserver` line of the resolver configuration names implicitly.
const DNS_PORT: u16 = 53;
/// The search-list entry the `resolv.conf` grammar uses to say the list is empty.
const EMPTY_SEARCH_LIST: &str = "--";

/// Where one node's resolver reads its configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsConfiguration {
    /// A `resolv.conf`-format file: name servers, search list, `ndots`, `timeout`, `attempts` and
    /// `edns0`.
    pub resolver_configuration: PathBuf,
    /// A hosts-format file consulted before DNS.
    pub hosts_file: PathBuf,
    /// Which name servers the resolver asks.
    pub name_servers: NameServers,
}

/// Which name servers a resolver asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameServers {
    /// Every `nameserver` line of the resolver configuration, each on port 53.
    ResolverConfiguration,
    /// These servers, in this order, in place of the resolver configuration's. The rest of the
    /// resolver configuration still applies.
    Explicit(Vec<SocketAddr>),
}

#[derive(Debug, Error)]
pub enum DnsConfigurationError {
    #[error("the resolver configuration '{}' could not be read", path.display())]
    ReadResolverConfiguration { path: PathBuf },
    #[error("the resolver configuration '{}' names no name server", path.display())]
    NoNameServers { path: PathBuf },
    #[error("the explicit name server list is empty")]
    EmptyNameServerList,
    #[error("the search domain '{domain}' is not a valid DNS name")]
    InvalidSearchDomain { domain: String },
    #[error("the hosts file '{}' could not be read", path.display())]
    ReadHostsFile { path: PathBuf },
    #[error("the resolver could not be constructed")]
    Build,
    #[error("loading the resolver configuration stopped before it finished")]
    Interrupted,
}

/// A resolver configuration read from disk, ready to construct a resolver from.
pub(crate) struct LoadedConfiguration {
    pub(crate) resolver: ResolverConfig,
    pub(crate) options: ResolverOpts,
    pub(crate) hosts: HostsTable,
}

impl DnsConfiguration {
    /// The host's own resolver configuration, hosts file and name servers.
    pub fn system() -> Self {
        Self {
            resolver_configuration: PathBuf::from(SYSTEM_RESOLVER_CONFIGURATION),
            hosts_file: PathBuf::from(SYSTEM_HOSTS_FILE),
            name_servers: NameServers::ResolverConfiguration,
        }
    }

    /// Read both files. Blocking file I/O: callers run it on the blocking pool.
    pub(crate) fn read(self) -> Result<LoadedConfiguration, Report<DnsConfigurationError>> {
        let file = ResolverConfigurationFile::read(&self.resolver_configuration)?;
        let name_servers = file.name_servers(&self.name_servers)?;
        let search = file.search_list()?;
        let resolver = ResolverConfig::from_parts(None, search, name_servers);
        let options = file.options();
        let hosts = HostsTable::read(&self.hosts_file)?;
        Ok(LoadedConfiguration {
            resolver,
            options,
            hosts,
        })
    }
}

/// One parsed `resolv.conf`-format file and where it was read from.
struct ResolverConfigurationFile {
    path: PathBuf,
    parsed: resolv_conf::Config,
}

impl ResolverConfigurationFile {
    /// Read and parse the file at `path`. A line the grammar cannot read is ignored with a warning,
    /// as the C library ignores it; a file that cannot be read at all is an error.
    fn read(path: &Path) -> Result<Self, Report<DnsConfigurationError>> {
        let bytes = fs::read(path).map_err(|error| {
            Report::new(DnsConfigurationError::ReadResolverConfiguration {
                path: path.to_path_buf(),
            })
            .attach_printable(error)
        })?;
        let (parsed, errors) = resolv_conf::Config::parse_with_errors(&bytes);
        for error in errors {
            warn!(
                path = %path.display(),
                %error,
                "ignored a line of the resolver configuration"
            );
        }
        Ok(Self {
            path: path.to_path_buf(),
            parsed,
        })
    }

    /// The name servers to ask: the explicit list when one is given, otherwise every `nameserver`
    /// line. Either way at least one server remains, or the configuration is rejected.
    fn name_servers(
        &self,
        selection: &NameServers,
    ) -> Result<Vec<NameServerConfig>, Report<DnsConfigurationError>> {
        let mut servers = Vec::new();
        match selection {
            NameServers::ResolverConfiguration => {
                for scoped in &self.parsed.nameservers {
                    let ip = IpAddr::from(scoped);
                    servers.push(name_server(SocketAddr::new(ip, DNS_PORT)));
                }
                if servers.is_empty() {
                    return Err(Report::new(DnsConfigurationError::NoNameServers {
                        path: self.path.clone(),
                    }));
                }
            }
            NameServers::Explicit(addresses) => {
                for address in addresses {
                    servers.push(name_server(*address));
                }
                if servers.is_empty() {
                    return Err(Report::new(DnsConfigurationError::EmptyNameServerList));
                }
            }
        }
        Ok(servers)
    }

    /// The domains an unqualified name is completed with: the last `search` or `domain` line, or,
    /// when the file has neither, the domain of this host's name, as the C library does.
    fn search_list(&self) -> Result<Vec<Name>, Report<DnsConfigurationError>> {
        let mut search = Vec::new();
        for domain in self.parsed.get_last_search_or_domain() {
            if domain == EMPTY_SEARCH_LIST {
                continue;
            }
            let name = Name::from_str_relaxed(domain).map_err(|error| {
                Report::new(DnsConfigurationError::InvalidSearchDomain {
                    domain: domain.clone(),
                })
                .attach_printable(error)
            })?;
            search.push(name);
        }
        let has_directive =
            self.parsed.get_search().is_some() || self.parsed.get_domain().is_some();
        if has_directive {
            return Ok(search);
        }
        // A host name that is not a valid DNS name contributes no search domain, as it does not
        // for the C library.
        if let Some(domain) = self.parsed.get_system_domain()
            && let Ok(name) = Name::from_str(&domain)
        {
            search.push(name);
        }
        Ok(search)
    }

    /// Hickory's options: the file's `ndots`, `timeout`, `attempts` and `edns0`, under this crate's
    /// cache, TTL and address-family policy. Hosts-file lookups belong to [`HostsTable`], so the
    /// resolver itself never reads one.
    fn options(&self) -> ResolverOpts {
        let mut options = ResolverOpts::default();
        options.ndots = self.parsed.ndots.arch_into();
        options.timeout = Duration::from_secs(u64::from(self.parsed.timeout));
        // The file counts tries of each server and Hickory counts retries after the first. A file
        // that asks for no tries still gets the first one.
        options.attempts = match self.parsed.attempts.checked_sub(1) {
            Some(retries) => retries.arch_into(),
            None => 0,
        };
        options.edns0 = self.parsed.edns0;
        options.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
        options.cache_size = CACHE_ENTRIES;
        options.positive_max_ttl = Some(POSITIVE_MAX_TTL);
        options.negative_max_ttl = Some(NEGATIVE_MAX_TTL);
        options.use_hosts_file = ResolveHosts::Never;
        options
    }
}

/// The name server at `address`, asked over UDP and, for a truncated answer, over TCP. A negative
/// answer from it is trusted rather than asked of the next server.
fn name_server(address: SocketAddr) -> NameServerConfig {
    let mut udp = ConnectionConfig::udp();
    udp.port = address.port();
    let mut tcp = ConnectionConfig::tcp();
    tcp.port = address.port();
    NameServerConfig::new(address.ip(), true, vec![udp, tcp])
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use meticulous::ResultExt as _;
    use tempfile::NamedTempFile;

    use super::*;

    fn file(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().assured("a temporary file can be created");
        file.write_all(contents.as_bytes())
            .assured("a temporary file can be written");
        file
    }

    fn configuration(
        resolver: &NamedTempFile,
        hosts: &NamedTempFile,
        name_servers: NameServers,
    ) -> DnsConfiguration {
        DnsConfiguration {
            resolver_configuration: resolver.path().to_path_buf(),
            hosts_file: hosts.path().to_path_buf(),
            name_servers,
        }
    }

    fn read_error(configuration: DnsConfiguration) -> Report<DnsConfigurationError> {
        match configuration.read() {
            Ok(_) => panic!("the configuration must be rejected"),
            Err(report) => report,
        }
    }

    #[test]
    fn name_servers_come_from_the_file_on_port_53() {
        let resolver = file("nameserver 192.0.2.1\nnameserver 2001:db8::1\n");
        let hosts = file("");
        let loaded = configuration(&resolver, &hosts, NameServers::ResolverConfiguration)
            .read()
            .assured("the configuration names two servers");
        let servers = loaded.resolver.name_servers();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].ip, IpAddr::from([192, 0, 2, 1]));
        assert!(servers[0].connections.iter().all(|c| c.port == DNS_PORT));
    }

    #[test]
    fn explicit_name_servers_replace_the_file_and_keep_their_ports() {
        let resolver = file("nameserver 192.0.2.1\nsearch example.test\n");
        let hosts = file("");
        let explicit = SocketAddr::from(([127, 0, 0, 1], 5353));
        let loaded = configuration(&resolver, &hosts, NameServers::Explicit(vec![explicit]))
            .read()
            .assured("the explicit server replaces the file's");
        let servers = loaded.resolver.name_servers();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].ip, explicit.ip());
        assert!(servers[0].connections.iter().all(|c| c.port == 5353));
        let search = loaded.resolver.search();
        assert_eq!(search.len(), 1);
        assert_eq!(search[0].to_ascii().trim_end_matches('.'), "example.test");
    }

    #[test]
    fn a_file_without_name_servers_is_rejected() {
        let resolver = file("search example.test\n");
        let hosts = file("");
        let report = read_error(configuration(
            &resolver,
            &hosts,
            NameServers::ResolverConfiguration,
        ));
        assert!(matches!(
            report.current_context(),
            DnsConfigurationError::NoNameServers { .. }
        ));
    }

    #[test]
    fn an_empty_explicit_list_is_rejected() {
        let resolver = file("nameserver 192.0.2.1\n");
        let hosts = file("");
        let report = read_error(configuration(
            &resolver,
            &hosts,
            NameServers::Explicit(Vec::new()),
        ));
        assert!(matches!(
            report.current_context(),
            DnsConfigurationError::EmptyNameServerList
        ));
    }

    #[test]
    fn a_missing_resolver_configuration_is_rejected() {
        let hosts = file("");
        let directory = tempfile::tempdir().assured("a temporary directory can be created");
        let report = read_error(DnsConfiguration {
            resolver_configuration: directory.path().join("missing.conf"),
            hosts_file: hosts.path().to_path_buf(),
            name_servers: NameServers::ResolverConfiguration,
        });
        assert!(matches!(
            report.current_context(),
            DnsConfigurationError::ReadResolverConfiguration { .. }
        ));
    }

    #[test]
    fn a_missing_hosts_file_is_rejected() {
        let resolver = file("nameserver 192.0.2.1\n");
        let directory = tempfile::tempdir().assured("a temporary directory can be created");
        let report = read_error(DnsConfiguration {
            resolver_configuration: resolver.path().to_path_buf(),
            hosts_file: directory.path().join("missing-hosts"),
            name_servers: NameServers::ResolverConfiguration,
        });
        assert!(matches!(
            report.current_context(),
            DnsConfigurationError::ReadHostsFile { .. }
        ));
    }

    #[test]
    fn an_invalid_search_domain_is_rejected() {
        let resolver = file("nameserver 192.0.2.1\nsearch bad..domain\n");
        let hosts = file("");
        let report = read_error(configuration(
            &resolver,
            &hosts,
            NameServers::ResolverConfiguration,
        ));
        assert!(matches!(
            report.current_context(),
            DnsConfigurationError::InvalidSearchDomain { .. }
        ));
    }

    #[test]
    fn options_follow_the_file_under_the_crate_policy() {
        let resolver = file("nameserver 192.0.2.1\noptions ndots:3 timeout:2 attempts:4 edns0\n");
        let hosts = file("");
        let loaded = configuration(&resolver, &hosts, NameServers::ResolverConfiguration)
            .read()
            .assured("the configuration is valid");
        let options = loaded.options;
        assert_eq!(options.ndots, 3);
        assert_eq!(options.timeout, Duration::from_secs(2));
        assert_eq!(options.attempts, 3);
        assert!(options.edns0);
        assert_eq!(options.ip_strategy, LookupIpStrategy::Ipv4AndIpv6);
        assert_eq!(options.cache_size, CACHE_ENTRIES);
        assert_eq!(options.positive_max_ttl, Some(POSITIVE_MAX_TTL));
        assert_eq!(options.negative_max_ttl, Some(NEGATIVE_MAX_TTL));
        assert_eq!(options.use_hosts_file, ResolveHosts::Never);
    }

    #[test]
    fn the_last_search_or_domain_line_is_the_search_list() {
        let resolver = file("nameserver 192.0.2.1\nsearch one.test two.test\ndomain three.test\n");
        let hosts = file("");
        let loaded = configuration(&resolver, &hosts, NameServers::ResolverConfiguration)
            .read()
            .assured("the configuration is valid");
        let search = loaded.resolver.search();
        assert_eq!(search.len(), 1);
        assert_eq!(search[0].to_ascii().trim_end_matches('.'), "three.test");
    }
}
