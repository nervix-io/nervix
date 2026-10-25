//! Who a node is, where it listens and is reached, and the files and directories it uses.
//!
//! Layer: edges.
//!
//! - **Owns.** The node's identity, its listen and advertised addresses, its TLS files, its name
//!   resolution files and its storage paths, resolved from the command line. None of them is a
//!   setting: they are what a node is provisioned with, not policy a class governs.
//! - **Depends on.** The vocabulary's node names and endpoints, and the resolver configuration.
//! - **Must not know.** Settings, or what the node runs once it has started.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{net::SocketAddr, path::PathBuf};

use nervix_dns::DnsConfiguration;
use nervix_models::{ClusterNodeName, NodeEndpoint};
use typed_builder::TypedBuilder;

use super::{InternalTransportMode, authentication::DEFAULT_USER};

/// Who a node is, where it listens and is reached, and the files and directories it uses.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NodeIdentity {
    pub addr: SocketAddr,
    #[builder(default)]
    pub grpc_mode: InternalTransportMode,
    #[builder(default)]
    pub grpc_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub grpc_https_advertise_addr: Option<NodeEndpoint>,
    pub http_listen_addr: SocketAddr,
    pub https_listen_addr: SocketAddr,
    pub observability_listen_addr: SocketAddr,
    /// An in-process node a test builds listens on a port the system chooses unless it says
    /// otherwise; the command line's default is a fixed public port.
    #[builder(default = SocketAddr::from(([127, 0, 0, 1], 0)))]
    pub web_console_listen_addr: SocketAddr,
    #[builder(default)]
    pub web_console_advertise_addr: Option<NodeEndpoint>,
    #[builder(default)]
    pub web_console_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub web_console_tls_cert: Option<PathBuf>,
    #[builder(default)]
    pub web_console_tls_key: Option<PathBuf>,
    pub cluster_id: String,
    pub node_id: ClusterNodeName,
    pub grpc_advertise_addr: NodeEndpoint,
    pub interconnect_listen_addr: SocketAddr,
    pub interconnect_advertise_addr: NodeEndpoint,
    pub interconnect_tls_ca: PathBuf,
    pub interconnect_tls_cert: PathBuf,
    pub interconnect_tls_key: PathBuf,
    pub allow_bootstrap: bool,
    #[builder(default = DEFAULT_USER.to_string())]
    pub default_user: String,
    #[builder(default)]
    pub init_default_user_password: Option<String>,
    pub cluster_bootstrap_host: Option<String>,
    #[builder(default = DnsConfiguration::system())]
    pub dns: DnsConfiguration,
    pub db_path: PathBuf,
    #[builder(default = PathBuf::from(crate::runtime::DEFAULT_TEMP_DIR))]
    pub temp_dir: PathBuf,
}
