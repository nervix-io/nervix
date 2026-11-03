//! Shared NATS connection-options preparation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Declared authentication and TLS options for every NATS client connection.
//! - **Depends on.** The connector contract, vocabulary declarations and the NATS driver.
//! - **Must not know.** Graph routing, subscriptions, publishing, placement or retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns NATS client option preparation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies resolved client entries")
)]

use async_nats::{ConnectOptions, ServerAddr};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{client_config_value, client_tls_paths, optional_client_config_value};
use nervix_models::{ClientConfigEntry, ConnectorKind};
use thiserror::Error;
use zeroize::Zeroizing;

mod authentication;

#[derive(Clone, Copy, Debug, Error)]
pub(crate) enum NatsClientOptionsError {
    #[error("invalid {connector} client configuration", connector = ConnectorKind::Nats)]
    Configuration,
    #[error("failed to parse the {connector} server address", connector = ConnectorKind::Nats)]
    Address,
    #[error("failed to load the {connector} credentials_file", connector = ConnectorKind::Nats)]
    CredentialsFile,
    #[error("failed to prepare the {connector} nkey_seed", connector = ConnectorKind::Nats)]
    NkeySeed,
}

pub(crate) struct NatsClientOptions {
    pub(crate) address: ServerAddr,
    pub(crate) options: ConnectOptions,
}

impl NatsClientOptions {
    pub(crate) async fn from_config(
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, NatsClientOptionsError> {
        let schema = ConnectorKind::Nats
            .client()
            .assured("NATS declares its configured client")
            .config;
        schema
            .validate_rendered(config)
            .change_context(NatsClientOptionsError::Configuration)?;
        let address = client_config_value(config, "addr", ConnectorKind::Nats.keyword())
            .change_context(NatsClientOptionsError::Configuration)?;
        let address = address
            .parse::<ServerAddr>()
            .change_context(NatsClientOptionsError::Address)?;
        let mut options = if let Some(token) = optional_client_config_value(config, "token") {
            ConnectOptions::new().token(token.to_owned())
        } else if let Some(seed) = optional_client_config_value(config, "nkey_seed") {
            authentication::nkey_options(seed).change_context(NatsClientOptionsError::NkeySeed)?
        } else if let Some(path) = optional_client_config_value(config, "credentials_file") {
            let path = path.to_owned();
            let contents =
                nervix_primitives::task::spawn_blocking(move || std::fs::read_to_string(path))
                    .await
                    .map_err(|source| {
                        Report::new(NatsClientOptionsError::CredentialsFile).attach(source)
                    })?
                    .map_err(|source| {
                        // A confidential path may occur in an I/O failure. Preserve its typed cause
                        // as an opaque attachment rather than rendering it in the public context.
                        Report::new(NatsClientOptionsError::CredentialsFile).attach(source)
                    })?;
            let contents = Zeroizing::new(contents);
            authentication::credentials_options(&contents)
                .change_context(NatsClientOptionsError::CredentialsFile)?
        } else {
            ConnectOptions::new()
        };
        options = options.require_tls(address.tls_required());
        let tls = client_tls_paths(config);
        if let Some(ca_file) = tls.ca_file {
            options = options.add_root_certificates(ca_file);
        }
        // The declared schema above already established a complete client identity or its absence.
        if let (Some(cert_file), Some(key_file)) = (tls.cert_file, tls.key_file) {
            options = options.add_client_certificate(cert_file, key_file);
        }
        Ok(Self { address, options })
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;
    use nervix_models::{ClientConfigEntry, ClientConfigSchemaError};

    use super::{NatsClientOptions, NatsClientOptionsError};

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[nervix_primitives::test]
    async fn preparation_preserves_the_driver_address_refusal() {
        let Err(report) =
            NatsClientOptions::from_config(&[entry("addr", "https://localhost:4222")]).await
        else {
            panic!("the NATS driver accepts only its transport schemes");
        };
        assert!(matches!(
            report.current_context(),
            NatsClientOptionsError::Address
        ));
        let source = report
            .downcast_ref::<std::io::Error>()
            .assured("the driver parsing failure remains typed");
        assert_eq!(source.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[nervix_primitives::test]
    async fn preparation_preserves_the_typed_schema_refusal() {
        let Err(report) = NatsClientOptions::from_config(&[
            entry("addr", "nats://localhost:4222"),
            entry("token", "test-token"),
            entry("nkey_seed", "test-seed"),
        ])
        .await
        else {
            panic!("two declared authentication methods cannot prepare a connection");
        };
        assert!(matches!(
            report.current_context(),
            NatsClientOptionsError::Configuration
        ));
        assert_eq!(
            report.downcast_ref::<ClientConfigSchemaError>(),
            Some(&ClientConfigSchemaError::MutuallyExclusive {
                first: "nkey_seed",
                second: "token"
            })
        );
    }

    #[nervix_primitives::test]
    async fn preparation_preserves_credentials_file_io_without_rendering_its_path() {
        let missing_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../docker/nats/no-such-credentials"
        );
        let Err(report) = NatsClientOptions::from_config(&[
            entry("addr", "nats://localhost:4222"),
            entry("credentials_file", missing_path),
        ])
        .await
        else {
            panic!("unavailable or malformed credentials cannot prepare a connection");
        };
        assert!(matches!(
            report.current_context(),
            NatsClientOptionsError::CredentialsFile
        ));
        let source = report
            .downcast_ref::<std::io::Error>()
            .assured("the filesystem failure remains typed");
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(
            report.to_string(),
            "failed to load the NATS credentials_file"
        );
        assert_eq!(
            format!("{report:#}"),
            "failed to load the NATS credentials_file"
        );
    }

    #[nervix_primitives::test]
    async fn preparation_preserves_the_typed_credentials_decoder_refusal() {
        let seed_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../docker/nats/auth-user.nk"
        );
        let Err(report) = NatsClientOptions::from_config(&[
            entry("addr", "nats://localhost:4222"),
            entry("credentials_file", seed_path),
        ])
        .await
        else {
            panic!("a credentials file requires a complete JWT and user seed");
        };
        assert!(matches!(
            report.current_context(),
            NatsClientOptionsError::CredentialsFile
        ));
        assert!(matches!(
            report.downcast_ref::<super::authentication::NatsAuthenticationError>(),
            Some(super::authentication::NatsAuthenticationError::Credentials)
        ));
        assert!(!format!("{report:#}").contains(seed_path));
    }
}
