//! The identity, address and path options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the node's identity, listen and advertised addresses,
//!   TLS files, name resolution files and storage paths, and their conversion into the node's
//!   resolved identity, including the addresses derived from others when they are not given.
//! - **Depends on.** The vocabulary's node names and endpoints, the resolver's configuration and
//!   the cluster's interconnect address derivation.
//! - **Must not know.** Settings, or what the node runs once it has started.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{net::SocketAddr, path::PathBuf};

use error_stack::{Report, ResultExt as _};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_models::{ClusterNodeName, NodeEndpoint};

use crate::{
    application::{
        InternalTransportMode, authentication::DEFAULT_USER, error::AppError,
        node_identity::NodeIdentity,
    },
    cluster,
};

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
        let interconnect_listen_addr = match args.interconnect_listen_addr.as_deref() {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .change_context(AppError::ParseInterconnectListenAddress)?,
            None => cluster::derive_peer_addr(addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let interconnect_advertise_addr = match args.interconnect_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<NodeEndpoint>().map_err(|error| {
                Report::new(AppError::ParseInterconnectAdvertiseAddress).attach_printable(error)
            })?,
            None => {
                let port = grpc_advertise_addr
                    .port()
                    .checked_add(1)
                    .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?;
                grpc_advertise_addr.with_port(port)
            }
        };
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
        Ok(Self::builder()
            .addr(addr)
            .grpc_mode(args.grpc_mode)
            .grpc_https_listen_addr(grpc_https_listen_addr)
            .grpc_https_advertise_addr(grpc_https_advertise_addr)
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
            .grpc_advertise_addr(grpc_advertise_addr)
            .interconnect_listen_addr(interconnect_listen_addr)
            .interconnect_advertise_addr(interconnect_advertise_addr)
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
