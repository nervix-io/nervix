//! The identity, address and path options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the node's identity, tags, listen and advertised
//!   addresses, TLS files, name resolution files and storage paths, and their conversion into the
//!   node's resolved identity, including the addresses derived from others when they are not
//!   given.
//! - **Depends on.** The vocabulary's node names, tags and endpoints, the resolver's configuration
//!   and the cluster's interconnect address derivation.
//! - **Must not know.** Settings, or what the node runs once it has started.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{net::SocketAddr, path::PathBuf};

use error_stack::{Report, ResultExt as _};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_models::{ClusterNodeName, NodeEndpoint, NodeTags};

use crate::{
    application::{
        InternalTransportMode,
        authentication::DEFAULT_USER,
        error::AppError,
        node_identity::{GrpcTransport, NodeIdentity},
    },
    cluster,
};

/// The option that provisions a node's tags, `--node-tags`.
pub const NODE_TAGS_OPTION: &str = "node-tags";

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Identity, addresses and paths")]
pub struct NodeArgs {
    #[arg(long, env = "NERVIX_ADDR", default_value = "127.0.0.1:47391")]
    pub addr: String,
    #[arg(long, env = "NERVIX_GRPC_MODE", value_enum, default_value_t)]
    pub grpc_mode: InternalTransportMode,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_LISTEN_ADDR")]
    pub grpc_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_ADVERTISE_ADDR")]
    pub grpc_https_advertise_addr: Option<String>,
    /// PEM certificate chain presented to native session clients in HTTPS mode; loaded at startup.
    #[arg(long, env = "NERVIX_GRPC_TLS_CERT")]
    pub grpc_tls_cert: Option<PathBuf>,
    /// Matching PEM private key for the native session certificate; loaded at startup.
    #[arg(long, env = "NERVIX_GRPC_TLS_KEY")]
    pub grpc_tls_key: Option<PathBuf>,
    #[arg(long, env = "NERVIX_HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub http_listen_addr: String,
    #[arg(long, env = "NERVIX_HTTPS_LISTEN_ADDR", default_value = "0.0.0.0:8443")]
    pub https_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_OBSERVABILITY_LISTEN_ADDR",
        default_value = "0.0.0.0:9090"
    )]
    pub observability_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_WEB_CONSOLE_LISTEN_ADDR",
        default_value = "0.0.0.0:47420"
    )]
    pub web_console_listen_addr: String,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_ADVERTISE_ADDR")]
    pub web_console_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_HTTPS_LISTEN_ADDR")]
    pub web_console_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_CERT")]
    pub web_console_tls_cert: Option<PathBuf>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_KEY")]
    pub web_console_tls_key: Option<PathBuf>,
    #[arg(long, env = "NERVIX_CLUSTER_ID", default_value = "default")]
    pub cluster_id: String,
    #[arg(long, env = "NERVIX_NODE_ID")]
    pub node_id: ClusterNodeName,
    /// Read as text and validated when the identity is resolved, so a refused tag ends the process
    /// as every other option relationship does.
    #[arg(
        long = NODE_TAGS_OPTION,
        env = "NERVIX_NODE_TAGS",
        value_delimiter = ',',
        help = "A tag this node carries in its record, as key=value; repeatable or comma-separated. \
                A key starts with a lower-case letter or a digit and holds lower-case letters, \
                digits, '.', '_' and '-', at most 63 bytes; a value is 1 to 255 bytes of printable \
                text without a comma; at most 64 tags, each key once"
    )]
    pub node_tags: Vec<String>,
    #[arg(long, env = "NERVIX_GRPC_ADVERTISE_ADDR")]
    pub grpc_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_LISTEN_ADDR")]
    pub interconnect_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_ADVERTISE_ADDR")]
    pub interconnect_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CA")]
    pub interconnect_tls_ca: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CERT")]
    pub interconnect_tls_cert: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_KEY")]
    pub interconnect_tls_key: PathBuf,
    #[arg(long, env = "NERVIX_ALLOW_BOOTSTRAP", default_value_t = false)]
    pub allow_bootstrap: bool,
    #[arg(long, env = "NERVIX_DEFAULT_USER", default_value = DEFAULT_USER)]
    pub default_user: String,
    #[arg(
        long,
        env = "NERVIX_INIT_DEFAULT_USER_PASSWORD",
        hide_env_values = true
    )]
    pub init_default_user_password: Option<String>,
    #[arg(long, env = "NERVIX_CLUSTER_BOOTSTRAP_HOST")]
    pub cluster_bootstrap_host: Option<String>,
    #[arg(
        long,
        env = "NERVIX_DNS_RESOLVER_CONFIG",
        default_value = nervix_dns::SYSTEM_RESOLVER_CONFIGURATION,
        help = "A resolv.conf-format file naming the name servers, search list and options the \
                node's resolver uses for peers, connectors and sessions"
    )]
    pub dns_resolver_config: PathBuf,
    #[arg(
        long,
        env = "NERVIX_DNS_HOSTS_FILE",
        default_value = nervix_dns::SYSTEM_HOSTS_FILE,
        help = "A hosts-format file consulted before DNS"
    )]
    pub dns_hosts_file: PathBuf,
    #[arg(
        long = "dns-name-server",
        env = "NERVIX_DNS_NAME_SERVERS",
        value_delimiter = ',',
        help = "A name server address with its port, replacing the resolver configuration's name \
                servers; repeatable or comma-separated"
    )]
    pub dns_name_servers: Vec<SocketAddr>,
    #[arg(long, env = "NERVIX_DB_PATH", default_value = "./.nervix-db")]
    pub db_path: String,
    #[arg(
        long,
        env = "NERVIX_TEMP_DIR",
        default_value = crate::runtime::DEFAULT_TEMP_DIR,
        help = "Directory used for local temporary files such as Iceberg emitter staging"
    )]
    pub temp_dir: PathBuf,
}

/// Where the node's interconnect listens and the endpoint it advertises to its peers.
struct InterconnectAddresses {
    listen_addr: SocketAddr,
    advertise_addr: NodeEndpoint,
}

impl InterconnectAddresses {
    /// The addresses the options give, or those derived from the session's: the interconnect
    /// listens beside the session address and advertises the port after the session's advertised
    /// one.
    fn resolve(
        listen_addr: Option<&str>,
        advertise_addr: Option<&str>,
        session_addr: SocketAddr,
        session_advertise_addr: &NodeEndpoint,
    ) -> Result<Self, Report<AppError>> {
        let listen_addr = match listen_addr {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .change_context(AppError::ParseInterconnectListenAddress)?,
            None => cluster::derive_peer_addr(session_addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let advertise_addr = match advertise_addr {
            Some(addr) => addr.parse::<NodeEndpoint>().map_err(|error| {
                Report::new(AppError::ParseInterconnectAdvertiseAddress).attach_printable(error)
            })?,
            None => {
                let port = session_advertise_addr
                    .port()
                    .checked_add(1)
                    .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?;
                session_advertise_addr.with_port(port)
            }
        };
        Ok(Self {
            listen_addr,
            advertise_addr,
        })
    }
}

impl TryFrom<NodeArgs> for NodeIdentity {
    type Error = Report<AppError>;

    fn try_from(args: NodeArgs) -> Result<Self, Self::Error> {
        let addr = args
            .addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseAddress)?;
        let grpc_https_listen_addr = args
            .grpc_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseGrpcHttpsListenAddress)
            })
            .transpose()?;
        let grpc_advertise_addr = match args.grpc_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<NodeEndpoint>().map_err(|error| {
                Report::new(AppError::ParseGrpcAdvertiseAddress).attach_printable(error)
            })?,
            None => addr.into(),
        };
        let grpc_https_advertise_addr = args
            .grpc_https_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<NodeEndpoint>().map_err(|error| {
                    Report::new(AppError::ParseGrpcHttpsAdvertiseAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let grpc_transport = match args.grpc_mode {
            InternalTransportMode::Http => {
                if args.grpc_tls_cert.is_some() || args.grpc_tls_key.is_some() {
                    return Err(Report::new(AppError::GrpcTlsRequiresHttps));
                }
                GrpcTransport::Http
            }
            InternalTransportMode::Https => GrpcTransport::Https {
                listen_addr: grpc_https_listen_addr
                    .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsListenAddress))?,
                advertise_addr: grpc_https_advertise_addr
                    .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsAdvertiseAddress))?,
                certificate: args
                    .grpc_tls_cert
                    .ok_or_else(|| Report::new(AppError::MissingGrpcTlsCertificate))?,
                private_key: args
                    .grpc_tls_key
                    .ok_or_else(|| Report::new(AppError::MissingGrpcTlsPrivateKey))?,
            },
        };
        let http_listen_addr = args
            .http_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpListenAddress)?;
        let https_listen_addr = args
            .https_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpsListenAddress)?;
        let observability_listen_addr = args
            .observability_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseObservabilityListenAddress)?;
        let web_console_listen_addr = args
            .web_console_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseWebConsoleListenAddress)?;
        let web_console_advertise_addr = args
            .web_console_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<NodeEndpoint>().map_err(|error| {
                    Report::new(AppError::ParseWebConsoleListenAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let web_console_https_listen_addr = args
            .web_console_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseWebConsoleHttpsListenAddress)
            })
            .transpose()?;
        let interconnect = InterconnectAddresses::resolve(
            args.interconnect_listen_addr.as_deref(),
            args.interconnect_advertise_addr.as_deref(),
            addr,
            &grpc_advertise_addr,
        )?;
        // An empty list is how the command line says no server was named, so the resolver
        // configuration's own servers apply.
        let name_servers = if args.dns_name_servers.is_empty() {
            NameServers::ResolverConfiguration
        } else {
            NameServers::Explicit(args.dns_name_servers)
        };
        let dns = DnsConfiguration {
            resolver_configuration: args.dns_resolver_config,
            hosts_file: args.dns_hosts_file,
            name_servers,
        };
        let tags = NodeTags::from_assignments(args.node_tags.iter().map(String::as_str))
            .change_context(AppError::InvalidNodeTags)?;
        Ok(Self::builder()
            .addr(addr)
            .grpc_transport(grpc_transport)
            .http_listen_addr(http_listen_addr)
            .https_listen_addr(https_listen_addr)
            .observability_listen_addr(observability_listen_addr)
            .web_console_listen_addr(web_console_listen_addr)
            .web_console_advertise_addr(web_console_advertise_addr)
            .web_console_https_listen_addr(web_console_https_listen_addr)
            .web_console_tls_cert(args.web_console_tls_cert)
            .web_console_tls_key(args.web_console_tls_key)
            .cluster_id(args.cluster_id)
            .node_id(args.node_id)
            .tags(tags)
            .grpc_advertise_addr(grpc_advertise_addr)
            .interconnect_listen_addr(interconnect.listen_addr)
            .interconnect_advertise_addr(interconnect.advertise_addr)
            .interconnect_tls_ca(args.interconnect_tls_ca)
            .interconnect_tls_cert(args.interconnect_tls_cert)
            .interconnect_tls_key(args.interconnect_tls_key)
            .allow_bootstrap(args.allow_bootstrap)
            .default_user(args.default_user)
            .init_default_user_password(args.init_default_user_password)
            .cluster_bootstrap_host(args.cluster_bootstrap_host)
            .dns(dns)
            .db_path(PathBuf::from(args.db_path))
            .temp_dir(args.temp_dir)
            .build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> (SocketAddr, NodeEndpoint) {
        let addr = "127.0.0.1:47391"
            .parse::<SocketAddr>()
            .expect("a literal socket address");
        (addr, NodeEndpoint::new("node-1.internal", 47391))
    }

    fn refusal(resolved: Result<InterconnectAddresses, Report<AppError>>) -> Report<AppError> {
        match resolved {
            Ok(_) => panic!("the interconnect addresses resolved"),
            Err(report) => report,
        }
    }

    #[test]
    fn the_interconnect_takes_the_addresses_it_is_given() {
        let (addr, advertised) = session();
        let resolved = InterconnectAddresses::resolve(
            Some("0.0.0.0:7000"),
            Some("peer.example:7001"),
            addr,
            &advertised,
        )
        .expect("given addresses resolve");
        assert_eq!(
            resolved.listen_addr,
            "0.0.0.0:7000".parse::<SocketAddr>().expect("a literal")
        );
        assert_eq!(
            resolved.advertise_addr,
            NodeEndpoint::new("peer.example", 7001)
        );
    }

    #[test]
    fn the_interconnect_listens_and_advertises_the_port_after_the_session_s() {
        let (addr, advertised) = session();
        let resolved = InterconnectAddresses::resolve(None, None, addr, &advertised)
            .expect("derived addresses resolve");
        assert_eq!(
            resolved.listen_addr,
            "127.0.0.1:47392".parse::<SocketAddr>().expect("a literal")
        );
        assert_eq!(
            resolved.advertise_addr,
            NodeEndpoint::new("node-1.internal", 47392)
        );
    }

    #[test]
    fn each_interconnect_address_that_cannot_be_read_or_derived_is_refused() {
        let (addr, advertised) = session();
        assert!(matches!(
            refusal(InterconnectAddresses::resolve(
                Some("not an address"),
                None,
                addr,
                &advertised
            ))
            .current_context(),
            AppError::ParseInterconnectListenAddress
        ));
        assert!(matches!(
            refusal(InterconnectAddresses::resolve(
                None,
                Some("no port"),
                addr,
                &advertised
            ))
            .current_context(),
            AppError::ParseInterconnectAdvertiseAddress
        ));
        let last_port = "127.0.0.1:65535"
            .parse::<SocketAddr>()
            .expect("a literal socket address");
        assert!(matches!(
            refusal(InterconnectAddresses::resolve(
                None,
                Some("peer.example:7001"),
                last_port,
                &advertised
            ))
            .current_context(),
            AppError::DeriveInterconnectAddress
        ));
        assert!(matches!(
            refusal(InterconnectAddresses::resolve(
                Some("0.0.0.0:7000"),
                None,
                addr,
                &NodeEndpoint::new("node-1.internal", u16::MAX)
            ))
            .current_context(),
            AppError::DeriveInterconnectAddress
        ));
    }
}
