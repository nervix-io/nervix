//! The one Pulsar client both directions connect through.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Turning a client's entries into a connected driver client: its address, the token
//!   or certificate it authenticates with, how much of a broker's certificate it confirms, and
//!   leaving every retry to the host; what connecting failed with, and what a broker answers when
//!   it refuses a request.
//! - **Depends on.** The connector contract's TLS configuration, typed client configuration
//!   entries, `error-stack` and `pulsar`.
//! - **Must not know.** Ingestors, emitters, topics, subscriptions, why a caller connects, or the
//!   host's retry cadence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::{fmt, time::Duration};

use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    RustlsClientConfigProvider, ServerCertificateTrust, client_config_value, client_tls_paths,
    optional_bool_client_config_value, optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ConnectorKind};
use pulsar::{
    Authentication, ConnectionRetryOptions, Error as PulsarError, OperationRetryOptions, Pulsar,
    PulsarBuilder, TokioExecutor,
    error::{
        ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError,
        ServiceDiscoveryError as PulsarServiceDiscoveryError,
    },
    message::proto::ServerError as PulsarServerError,
};
use thiserror::Error;
use url::Url;

use super::PULSAR_OUTBOUND_CAPACITY;

const PULSAR: ConnectorKind = ConnectorKind::Pulsar;

/// The address scheme of a TLS connection to a broker, under which the TLS keys mean something.
const TLS_SCHEME: &str = "pulsar+ssl";
/// The method a broker authenticates a token under.
const TOKEN_AUTHENTICATION: &str = "token";
/// The method a broker authenticates a client certificate under. The certificate itself is the
/// credential, so this method carries no data.
const TLS_AUTHENTICATION: &str = "tls";

/// What a Pulsar broker answered when it refused a request: its server error, absent when the
/// client does not know the error's code, and the reason it gave, absent when it gave none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulsarRefusal {
    pub error: Option<PulsarServerError>,
    pub reason: Option<String>,
}

impl fmt::Display for PulsarRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error {
            Some(error) => formatter.write_str(error.as_str_name())?,
            None => formatter.write_str("a server error this client does not know")?,
        }
        if let Some(reason) = &self.reason {
            write!(formatter, ": {reason}")?;
        }
        Ok(())
    }
}

impl PulsarRefusal {
    /// What the broker refused a join with, when `error` says it refused the subscribe or the
    /// topic lookup before it, and did not fail in some other way.
    pub(super) fn of_join(error: &PulsarError) -> Option<Self> {
        let (code, reason) = match error {
            PulsarError::Connection(PulsarConnectionError::PulsarError(code, reason))
            | PulsarError::Consumer(PulsarConsumerError::Connection(
                PulsarConnectionError::PulsarError(code, reason),
            ))
            | PulsarError::ServiceDiscovery(PulsarServiceDiscoveryError::Query(code, reason)) => {
                (code, reason)
            }
            _ => return None,
        };
        Some(Self {
            error: *code,
            reason: reason.clone(),
        })
    }

    /// Whether the broker refused for what holds right now rather than for what the statement
    /// declares, so the same join can pass later: another consumer holds the subscription, or the
    /// broker has not yet loaded the topic.
    pub(super) fn holds_only_for_now(&self) -> bool {
        matches!(
            self.error,
            Some(PulsarServerError::ConsumerBusy | PulsarServerError::ServiceNotReady)
        )
    }
}

/// Why a Pulsar client did not connect.
#[derive(Debug, Error)]
pub enum PulsarClientError {
    /// The client's entries do not describe a connection; the cause beneath says why.
    #[error("invalid {connector} client configuration", connector = ConnectorKind::Pulsar)]
    Configuration,
    /// The broker answered the connection by refusing it, such as a credential it does not accept.
    #[error(
        "the {connector} broker refused the connection with {refusal}",
        connector = ConnectorKind::Pulsar
    )]
    Refused { refusal: PulsarRefusal },
    /// No connection opened, such as an address nothing answers or a TLS handshake that failed;
    /// the client's error is the cause beneath.
    #[error("no connection to the {connector} broker opened", connector = ConnectorKind::Pulsar)]
    NotOpened,
}

impl PulsarClientError {
    /// What failing to connect with `error` means: the refusal the broker answered with, or a
    /// connection that never opened.
    fn connecting(error: PulsarError) -> Report<Self> {
        if let PulsarError::Connection(PulsarConnectionError::PulsarError(code, reason)) = &error {
            return Report::new(Self::Refused {
                refusal: PulsarRefusal {
                    error: *code,
                    reason: reason.clone(),
                },
            });
        }
        Report::new(error).change_context(Self::NotOpened)
    }
}

/// One connection a client's entries declare, with the operation timeout its user holds the driver
/// to. Both directions connect through this, so they cannot disagree on how.
pub(super) struct PulsarClientConnection<'a> {
    tls_policy: &'a nervix_connector::OutwardTlsPolicy,
    config: &'a [ClientConfigEntry],
    /// The time the driver gives one operation, or the driver's own 30 seconds when absent.
    operation_timeout: Option<Duration>,
}

pub(super) struct PreparedPulsarClient {
    address: String,
    authentication: Option<Authentication>,
    tls: Option<nervix_primitives::sync::StdArc<rustls::ClientConfig>>,
    operation_timeout: Option<Duration>,
}

impl PreparedPulsarClient {
    pub(super) async fn open(&self) -> Result<Pulsar<TokioExecutor>, Report<PulsarClientError>> {
        self.builder()
            .build()
            .await
            .map_err(PulsarClientError::connecting)
    }

    pub(super) fn builder(&self) -> PulsarBuilder<TokioExecutor> {
        let mut builder = Pulsar::builder(self.address.clone(), TokioExecutor)
            .with_connection_retry_options(PulsarClientConnection::connection_retries())
            .with_operation_retry_options(PulsarClientConnection::operation_retry_options(
                self.operation_timeout,
            ))
            .with_outbound_channel_size(PULSAR_OUTBOUND_CAPACITY);
        if let Some(auth) = &self.authentication {
            builder = builder.with_auth(auth.clone());
        }
        if let Some(tls) = &self.tls {
            builder = builder.with_rustls_client_config(tls.clone());
        }
        builder
    }
}

impl<'a> PulsarClientConnection<'a> {
    pub(super) fn new(
        config: &'a [ClientConfigEntry],
        operation_timeout: Option<Duration>,
        tls_policy: &'a nervix_connector::OutwardTlsPolicy,
    ) -> Self {
        Self {
            tls_policy,
            config,
            operation_timeout,
        }
    }

    /// Connects once. The client it returns keeps the host's retry contract: it retries neither a
    /// connection nor an operation, so what fails reaches the host, which alone decides when to
    /// try again.
    pub(super) async fn open(&self) -> Result<Pulsar<TokioExecutor>, Report<PulsarClientError>> {
        self.builder()?
            .build()
            .await
            .map_err(PulsarClientError::connecting)
    }

    pub(super) fn prepare(&self) -> Result<PreparedPulsarClient, Report<PulsarClientError>> {
        let address = client_config_value(self.config, "addr", PULSAR.keyword())
            .change_context(PulsarClientError::Configuration)?;
        let parsed = Url::parse(&address).change_context(PulsarClientError::Configuration)?;
        let tls = if parsed.scheme() == TLS_SCHEME {
            Some(
                RustlsClientConfigProvider::new(self.config, self.tls_policy)
                    .build_with_server_trust(self.server_certificate_trust()?)
                    .change_context(PulsarClientError::Configuration)?,
            )
        } else {
            None
        };
        Ok(PreparedPulsarClient {
            address,
            authentication: self.authentication(),
            tls,
            operation_timeout: self.operation_timeout,
        })
    }

    fn builder(&self) -> Result<PulsarBuilder<TokioExecutor>, Report<PulsarClientError>> {
        Ok(self.prepare()?.builder())
    }

    /// The driver makes one attempt at a connection.
    fn connection_retries() -> ConnectionRetryOptions {
        ConnectionRetryOptions {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// The driver makes one attempt at an operation, within this connection's operation timeout.
    pub(super) fn operation_retry_options(
        operation_timeout: Option<Duration>,
    ) -> OperationRetryOptions {
        let mut options = OperationRetryOptions {
            max_retries: Some(0),
            ..Default::default()
        };
        if let Some(operation_timeout) = operation_timeout {
            options.operation_timeout = operation_timeout;
        }
        options
    }

    /// What the client authenticates with. A token names the client's role. A certificate alone
    /// does too, under the method a broker reads the TLS session for. Beside a token, a certificate
    /// only satisfies a broker that requires one on connect.
    fn authentication(&self) -> Option<Authentication> {
        if let Some(token) = optional_client_config_value(self.config, "auth_token") {
            return Some(Authentication {
                name: TOKEN_AUTHENTICATION.to_string(),
                data: token.as_bytes().to_vec(),
            });
        }
        let presents_certificate = client_tls_paths(self.config).cert_file.is_some();
        if !presents_certificate {
            return None;
        }
        Some(Authentication {
            name: TLS_AUTHENTICATION.to_string(),
            data: Vec::new(),
        })
    }

    /// How much of a broker's certificate this client confirms.
    fn server_certificate_trust(
        &self,
    ) -> Result<ServerCertificateTrust, Report<PulsarClientError>> {
        let allow_insecure =
            optional_bool_client_config_value(self.config, "tls_allow_insecure_connection")
                .change_context(PulsarClientError::Configuration)?;
        if allow_insecure == Some(true) {
            return Ok(ServerCertificateTrust::Unconfirmed);
        }
        let verifies_hostname =
            optional_bool_client_config_value(self.config, "tls_hostname_verification_enabled")
                .change_context(PulsarClientError::Configuration)?;
        if verifies_hostname == Some(false) {
            return Ok(ServerCertificateTrust::Anchored);
        }
        Ok(ServerCertificateTrust::NamedAndAnchored)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "connection_tests.rs"]
mod tests;
