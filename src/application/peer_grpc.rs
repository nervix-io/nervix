//! How this node reaches another node's session service.
//!
//! Layer: edges.
//!
//! - **Owns.** The connect options a client uses to reach a peer's session service.
//! - **Depends on.** The internal TLS material and the credentials a client presents.
//! - **Must not know.** What the caller asks the peer to do.

use nervix_client_core::{
    ConnectOptions as ClientConnectOptions, TlsRequirement as ClientTlsRequirement,
};

use super::{
    authentication::BasicAuthCredentials,
    tls::{INTERNAL_TLS_CA_FILE, internal_tls_path},
};

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
        ..ClientConnectOptions::default()
    }
}
