//! The options one MQTT client connects to its broker with, for an ingestor instance and an emitter
//! alike.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Turning one rendered client configuration into the driver's connection options: the
//!   identity a connection presents, its broker and TLS, the username and password it authenticates
//!   with, and the keep-alive it asks for, with the dialer that reaches the broker through the node
//!   resolver installed once a side has completed the options.
//! - **Depends on.** The vocabulary's typed MQTT client configuration, the connector contract's TLS
//!   file reading, the crate's dialer, `error-stack` and `rumqttc`.
//! - **Must not know.** Sessions, subscriptions, acknowledgements, publishing modes, the host or
//!   Models.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::path::Path;

use error_stack::ResultExt as _;
use nervix_connector::read_tls_file;
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, MqttClientConfig, MqttTransport};
use rumqttc::{MqttOptions, TlsConfiguration, Transport as DriverTransport};
use thiserror::Error;

use super::connection::MqttDialer;

/// Why one MQTT client configuration yields no connection options.
#[derive(Debug, Error)]
pub enum MqttClientOptionsError {
    /// The vocabulary's refusal of the rendered configuration is beneath.
    #[error("invalid MQTT client configuration")]
    Configuration,
    /// The connector contract's report of the file it could not read is beneath.
    #[error("failed to read the MQTT client's {file}")]
    TlsFile { file: &'static str },
}

/// The connection options one MQTT client configuration declares, before a side completes them
/// with its session, quality of service and request capacity.
pub(crate) struct MqttClientOptions {
    config: MqttClientConfig,
    options: MqttOptions,
}

impl MqttClientOptions {
    /// The options `entries` declare. A connection presents the identity they render, or
    /// `default_client_id` where they declare none.
    pub(crate) fn from_config(
        entries: &[ClientConfigEntry],
        default_client_id: &str,
    ) -> error_stack::Result<Self, MqttClientOptionsError> {
        let config = MqttClientConfig::parse(entries)
            .change_context(MqttClientOptionsError::Configuration)?;
        let client_id = match config.client_id() {
            Some(client_id) => client_id,
            None => default_client_id,
        };
        let broker = config.broker();
        let mut options = MqttOptions::new(client_id, (broker.host(), broker.port().get()));
        options.set_keep_alive(config.keep_alive_seconds().get());
        match (config.username(), config.password()) {
            (Some(username), Some(password)) => {
                options.set_credentials(username, password.expose().to_owned());
            }
            (Some(username), None) => {
                options.set_username(username);
            }
            (None, Some(password)) => {
                options.set_password(password.expose().to_owned());
            }
            (None, None) => {}
        }
        if let MqttTransport::Tls(files) = config.transport() {
            let ca = Self::tls_file(files.certificate_authority(), "TLS CA certificate")?;
            let client_auth = match files.identity() {
                Some(identity) => Some((
                    Self::tls_file(identity.certificate(), "TLS certificate")?,
                    Self::tls_file(identity.private_key(), "TLS private key")?,
                )),
                None => None,
            };
            options.set_transport(DriverTransport::Tls(TlsConfiguration::Simple {
                ca,
                alpn: None,
                client_auth,
            }));
        }
        Ok(Self { config, options })
    }

    /// The bytes of the TLS file at `path`, which the configuration names as its `file`.
    fn tls_file(
        path: &Path,
        file: &'static str,
    ) -> error_stack::Result<Vec<u8>, MqttClientOptionsError> {
        read_tls_file(&path.to_path_buf(), file)
            .change_context(MqttClientOptionsError::TlsFile { file })
    }

    /// The configuration the options were built from.
    pub(crate) fn config(&self) -> &MqttClientConfig {
        &self.config
    }

    /// The options, for a side to complete with what only it decides.
    pub(crate) fn options_mut(&mut self) -> &mut MqttOptions {
        &mut self.options
    }

    /// The completed options, with the dialer every connection reaches the broker through. The
    /// dialer is installed last, so its budget is the connect timeout the driver enforces.
    pub(crate) fn dialing_through(mut self, dns: DnsResolver) -> MqttOptions {
        MqttDialer::install(&mut self.options, dns);
        self.options
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "client_options_tests.rs"]
mod tests;
