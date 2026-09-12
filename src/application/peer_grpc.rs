//! How this node reaches another node's session service.
//!
//! Layer: edges.
//!
//! - **Owns.** The gRPC base URL for a peer and the connect options a client uses to reach it.
//! - **Depends on.** The configured internal transport mode and its TLS material.
//! - **Must not know.** What the caller asks the peer to do.

use nervix_client_core::{
    ConnectOptions as ClientConnectOptions, TlsRequirement as ClientTlsRequirement,
};

use super::{
    InternalTransportMode,
    authentication::BasicAuthCredentials,
    tls::{INTERNAL_TLS_CA_FILE, internal_tls_path},
};
use crate::cluster;
pub(in crate::application) fn grpc_uri_from_advertise_addr(addr: &str) -> Option<String> {
    if addr.is_empty() {
        None
    } else if addr.starts_with("http://") || addr.starts_with("https://") {
        Some(addr.to_string())
    } else {
        Some(format!("http://{addr}"))
    }
}

pub(in crate::application) fn grpc_client_connect_options(
    server: &str,
    credentials: Option<&BasicAuthCredentials>,
) -> ClientConnectOptions {
    ClientConnectOptions {
        tls_requirement: Some(ClientTlsRequirement::Preferred),
        ca_certificate_pem: server
            .starts_with("https://")
            .then(|| std::fs::read(internal_tls_path(INTERNAL_TLS_CA_FILE)).ok())
            .flatten(),
        username: credentials.map(|credentials| credentials.username.clone()),
        password: credentials.map(|credentials| credentials.password.clone()),
    }
}

pub(in crate::application) fn grpc_base_url(
    mode: InternalTransportMode,
    advertise_addr: &cluster::HostPort,
) -> String {
    format!("{}://{}", mode.scheme(), advertise_addr.url_authority())
}
