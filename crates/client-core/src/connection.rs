//! Reaching a server: TLS selection, credentials, and the servers a client has learned about.
//!
//! - **Owns.** The connect options, the channel a server is reached over, the `authorization`
//!   metadata every call carries, and the directory of servers a lost session reconnects to.
//! - **Depends on.** tonic's transport and rustls.
//! - **Must not know.** What the calls on a channel carry.

use std::{str::FromStr as _, time::Duration};

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

#[derive(Debug, thiserror::Error)]
pub(crate) enum EndpointValidationError {
    #[error("the server URL is not a plain HTTP or HTTPS origin")]
    InvalidOrigin,
    #[error("the server URL does not satisfy the TLS requirement")]
    TlsRequired,
}

impl From<error_stack::Report<EndpointValidationError>> for ClientError {
    fn from(report: error_stack::Report<EndpointValidationError>) -> Self {
        match report.current_context() {
            EndpointValidationError::InvalidOrigin => Self::InvalidServerEndpoint,
            EndpointValidationError::TlsRequired => Self::TlsRequired,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsRequirement {
    Preferred,
    Required,
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub tls_requirement: Option<TlsRequirement>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Additional configured gRPC endpoints available for initial connection and recovery.
    pub seed_servers: Vec<Url>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub retry_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            tls_requirement: None,
            ca_certificate_pem: None,
            username: None,
            password: None,
            seed_servers: Vec::new(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            retry_timeout: Duration::from_secs(120),
        }
    }
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
        self.validate_server(server)?;
        let is_https = server.scheme() == "https";
        let mut endpoint = Channel::from_shared(server.as_str().to_string())
            .map_err(ClientError::InvalidServerUri)?;
        endpoint = endpoint.connect_timeout(self.options.connect_timeout);
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

    pub(crate) fn validate_server(
        &self,
        server: &Url,
    ) -> error_stack::Result<(), EndpointValidationError> {
        if !matches!(server.scheme(), "http" | "https")
            || !server.has_host()
            || !server.username().is_empty()
            || server.password().is_some()
            || server.path() != "/"
            || server.query().is_some()
            || server.fragment().is_some()
        {
            return Err(error_stack::Report::new(
                EndpointValidationError::InvalidOrigin,
            ));
        }
        if self.options.tls_requirement == Some(TlsRequirement::Required)
            && server.scheme() != "https"
        {
            return Err(error_stack::Report::new(
                EndpointValidationError::TlsRequired,
            ));
        }
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.options.request_timeout
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        self.options.connect_timeout
    }

    pub(crate) fn retry_timeout(&self) -> Duration {
        self.options.retry_timeout
    }

    pub(crate) fn seed_servers(&self) -> &[Url] {
        &self.options.seed_servers
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

/// The configured seeds and bounded discovered endpoints a client can reconnect to.
#[derive(Debug, Default)]
pub(crate) struct ServerDirectory {
    current: Option<Url>,
    seeds: IndexSet<Url>,
    discovered: IndexSet<Url>,
}

impl ServerDirectory {
    const MAX_DISCOVERED_ENDPOINTS: usize = 32;

    /// A directory whose only server is the one the first exchange runs on, when it is known.
    pub(crate) fn connected_to(server: Option<Url>) -> Self {
        let mut directory = Self::default();
        if let Some(server) = server {
            directory.seeds.insert(server.clone());
            directory.current = Some(server);
        }
        directory
    }

    pub(crate) fn with_seeds(primary: Url, seeds: &[Url]) -> Self {
        let mut directory = Self::default();
        directory.seeds.insert(primary);
        for seed in seeds {
            directory.seeds.insert(seed.clone());
        }
        directory
    }

    pub(crate) fn remember(&mut self, server: &Url) {
        if self.seeds.contains(server) || self.discovered.contains(server) {
            return;
        }
        if self.discovered.len() == Self::MAX_DISCOVERED_ENDPOINTS {
            self.discovered
                .shift_remove_index(0)
                .discarded("the oldest discovered endpoint gives way to a fresh one");
        }
        self.discovered.insert(server.clone());
    }

    /// Records that the exchange now runs on `server`.
    pub(crate) fn connected(&mut self, server: &Url) {
        self.current = Some(server.clone());
        self.remember(server);
    }

    /// The servers to try when the exchange is lost: every known server other than the current
    /// one, in the order they were learned, and then the current one.
    pub(crate) fn reconnect_candidates(&self) -> Vec<Url> {
        let mut candidates = Vec::with_capacity(self.seeds.len() + self.discovered.len());
        for server in self.seeds.iter().chain(&self.discovered) {
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
