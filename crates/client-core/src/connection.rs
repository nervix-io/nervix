//! Reaching a server: TLS selection, credentials, and the servers a client has learned about.
//!
//! - **Owns.** The connect options, the channel a server is reached over, the `authorization`
//!   metadata every call carries, and the directory of servers a lost session reconnects to.
//! - **Depends on.** tonic's transport and rustls.
//! - **Must not know.** What the calls on a channel carry.

use std::str::FromStr as _;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use indexmap::IndexSet;
use nervix_recovery::Discarded as _;
use rustls::crypto::aws_lc_rs;
use tonic::{
    Request,
    metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig},
};
use url::Url;

use crate::error::ClientError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsRequirement {
    Preferred,
    Required,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub tls_requirement: Option<TlsRequirement>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ConnectOptions {
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    fn basic_authorization(&self) -> Option<String> {
        let username = self.username.as_ref()?;
        let password = self.password.as_ref()?;
        let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
        Some(format!("Basic {encoded}"))
    }
}

/// Opens channels with one client's TLS policy and presents its credentials on every call.
#[derive(Clone)]
pub(crate) struct GrpcConnector {
    options: ConnectOptions,
    /// The `authorization` metadata, built once from the credentials when the client has them.
    authorization: Option<AsciiMetadataValue>,
}

impl GrpcConnector {
    pub(crate) fn new(options: ConnectOptions) -> Result<Self, InvalidMetadataValue> {
        let authorization = match options.basic_authorization() {
            Some(authorization) => Some(AsciiMetadataValue::from_str(&authorization)?),
            None => None,
        };
        Ok(Self {
            options,
            authorization,
        })
    }

    pub(crate) async fn connect(&self, server: &Url) -> Result<Channel, ClientError> {
        let tls_requirement = self
            .options
            .tls_requirement
            .unwrap_or(TlsRequirement::Preferred);
        let is_https = server.scheme() == "https";
        if tls_requirement == TlsRequirement::Required && !is_https {
            return Err(ClientError::TlsRequired);
        }
        let mut endpoint = Channel::from_shared(server.as_str().to_string())
            .map_err(ClientError::InvalidServerUri)?;
        if is_https {
            aws_lc_rs::default_provider().install_default().discarded(
                "a provider another client installed first is the one this client would have \
                 installed",
            );
            let mut tls = ClientTlsConfig::new();
            if let Some(pem) = self.options.ca_certificate_pem.clone() {
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(ClientError::ConfigureTls)?;
        }
        endpoint.connect().await.map_err(ClientError::ConnectServer)
    }

    /// Presents the client's credentials on a call.
    pub(crate) fn authorize<T>(&self, request: &mut Request<T>) {
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
    }
}

/// The servers a client knows about: the one its exchange runs on, and every server it connected
/// to or was redirected to, in the order it learned them.
#[derive(Debug, Default)]
pub(crate) struct ServerDirectory {
    current: Option<Url>,
    known: IndexSet<Url>,
}

impl ServerDirectory {
    /// A directory whose only server is the one the first exchange runs on, when it is known.
    pub(crate) fn connected_to(server: Option<Url>) -> Self {
        let mut directory = Self::default();
        if let Some(server) = server {
            directory.connected(&server);
        }
        directory
    }

    pub(crate) fn remember(&mut self, server: &Url) {
        if !self.known.contains(server) {
            self.known.insert(server.clone());
        }
    }

    /// Records that the exchange now runs on `server`.
    pub(crate) fn connected(&mut self, server: &Url) {
        self.current = Some(server.clone());
        self.remember(server);
    }

    /// The servers to try when the exchange is lost: every known server other than the current
    /// one, in the order they were learned, and then the current one.
    pub(crate) fn reconnect_candidates(&self) -> Vec<Url> {
        let mut candidates = Vec::with_capacity(self.known.len());
        for server in &self.known {
            if self.current.as_ref() != Some(server) {
                candidates.push(server.clone());
            }
        }
        if let Some(current) = &self.current {
            candidates.push(current.clone());
        }
        candidates
    }
}
