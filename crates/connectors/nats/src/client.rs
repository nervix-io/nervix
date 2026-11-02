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

#[derive(Clone, Copy, Debug, Error)]
pub(crate) enum NatsClientOptionsError {
    #[error("invalid {connector} client configuration", connector = ConnectorKind::Nats)]
    Configuration,
    #[error("failed to parse the {connector} server address", connector = ConnectorKind::Nats)]
    Address,
    #[error("failed to load the {connector} credentials_file", connector = ConnectorKind::Nats)]
    CredentialsFile,
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
        let mut options = ConnectOptions::new().require_tls(address.tls_required());
        let tls = client_tls_paths(config);
        if let Some(ca_file) = tls.ca_file {
            options = options.add_root_certificates(ca_file);
        }
        // The declared schema above already established a complete client identity or its absence.
        if let (Some(cert_file), Some(key_file)) = (tls.cert_file, tls.key_file) {
            options = options.add_client_certificate(cert_file, key_file);
        }
        if let Some(token) = optional_client_config_value(config, "token") {
            options = options.token(token.to_owned());
        }
        if let Some(seed) = optional_client_config_value(config, "nkey_seed") {
            options = options.nkey(seed.to_owned());
        }
        if let Some(path) = optional_client_config_value(config, "credentials_file") {
            options = options.credentials_file(path).await.map_err(|source| {
                // The driver's I/O error may name this confidential path. Keep the typed
                // failure inspectable without rendering it as a public report context.
                Report::new(NatsClientOptionsError::CredentialsFile).attach(source)
            })?;
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
    async fn preparation_preserves_credentials_file_io_and_decoding_failures() {
        // This public nkey fixture is a seed, rather than a complete credentials file. Its
        // contents are intentionally unsuitable for the driver's credentials-file decoder.
        let seed_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../docker/nats/auth-user.nk"
        );
        let missing_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../docker/nats/no-such-credentials"
        );
        for (path, kind) in [
            (seed_path, std::io::ErrorKind::InvalidData),
            // The driver wraps a filesystem read failure in io::Error::other; its decoder
            // reports InvalidData. Preserve exactly the typed failure its public API returns.
            (missing_path, std::io::ErrorKind::Other),
        ] {
            let Err(report) = NatsClientOptions::from_config(&[
                entry("addr", "nats://localhost:4222"),
                entry("credentials_file", path),
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
                .assured("the driver IO failure remains typed");
            assert_eq!(source.kind(), kind);
            assert_eq!(
                report.to_string(),
                "failed to load the NATS credentials_file"
            );
            assert_eq!(
                format!("{report:#}"),
                "failed to load the NATS credentials_file"
            );
        }
    }
}
