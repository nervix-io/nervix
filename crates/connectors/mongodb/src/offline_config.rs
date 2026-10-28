//! MongoDB configuration interpretation before a client opens.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Native address/option parsing, the resolved database and pool/TLS-option refusals.
//! - **Depends on.** The connector contract, vocabulary declarations and MongoDB driver.
//! - **Must not know.** Models, transaction admission, resource installation or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns MongoDB configuration interpretation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies resolved client entries")
)]

use std::future::IntoFuture as _;

use error_stack::{Report, ResultExt as _};
use futures_util::FutureExt as _;
use meticulous::OptionExt as _;
use mongodb::options::{ClientOptions, ConnectionString, HostInfo};
use nervix_connector::{
    OfflineClientConfigError, client_config_value, optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ConnectorKind};

use crate::tls_identity::MongoDbClientOptions;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "MongoDB database names must be shorter than 64 UTF-8 bytes"
    )
)]
const MAX_DATABASE_BYTES: usize = 63;

/// A successfully parsed driver configuration and its required, validated database.
pub(crate) struct MongoDbClientConfiguration {
    options: MongoDbClientOptions,
    database: String,
}

impl MongoDbClientConfiguration {
    pub(crate) fn parse(
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, OfflineClientConfigError> {
        ConnectorKind::MongoDb
            .client()
            .assured("MongoDB declares a client")
            .config
            .validate_rendered(config)
            .change_context(OfflineClientConfigError::Configuration)?;
        let address = client_config_value(config, "addr", ConnectorKind::MongoDb.keyword())
            .change_context(OfflineClientConfigError::key("addr"))?;
        let connection = ConnectionString::parse(address)
            .change_context(OfflineClientConfigError::key("addr"))?;
        // Only explicit seed addresses belong to this declaration. Check the driver's typed
        // address form before polling its parser, so parsing never initiates DNS discovery.
        let HostInfo::HostIdentifiers(_) = &connection.host_info else {
            return Err(Report::new(OfflineClientConfigError::key("addr")));
        };
        for (key, value) in [
            ("maxPoolSize", connection.max_pool_size),
            ("minPoolSize", connection.min_pool_size),
        ] {
            if value.is_some() {
                return Err(Report::new(OfflineClientConfigError::PoolSizing {
                    key: key.to_owned(),
                }));
            }
        }
        // The driver's explicit-seed parse performs pure validation and returns immediately.
        // Poll once so inspection needs no runtime or asynchronous wait. An unexpectedly
        // pending parse is a typed library failure.
        let Some(parsed) = ClientOptions::parse(connection)
            .into_future()
            .now_or_never()
        else {
            return Err(Report::new(OfflineClientConfigError::IncompleteInspection));
        };
        let mut options = parsed.change_context(OfflineClientConfigError::key("addr"))?;
        let database = if let Some(database) = optional_client_config_value(config, "database") {
            database.to_string()
        } else if let Some(database) = options.default_database.take() {
            database
        } else {
            return Err(Report::new(OfflineClientConfigError::key("database")));
        };
        if database.is_empty()
            || database.len() > MAX_DATABASE_BYTES
            || database
                .chars()
                .any(|ch| matches!(ch, '\0' | '/' | '\\' | '.' | ' ' | '"' | '$'))
        {
            return Err(Report::new(OfflineClientConfigError::key("database")));
        }
        // The emitter selects this required database explicitly. The driver holds no second
        // independently optional copy of the database this configuration already resolved.
        options.default_database = None;
        let options = MongoDbClientOptions::new(options, config)
            .change_context(OfflineClientConfigError::key("addr"))?;
        Ok(Self { options, database })
    }

    pub(crate) fn into_options(self) -> MongoDbClientOptions {
        self.options
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }
}

/// Validate the complete current configuration without opening a connection or resolving a host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    MongoDbClientConfiguration::parse(config)?;
    Ok(())
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns native configuration parser evidence")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Complete native option and database validation checks.
    //! - **Depends on.** The production configuration owner and connector vocabulary.
    //! - **Must not know.** Cluster admission or runtime graph execution.

    use meticulous::ResultExt as _;

    use super::*;

    fn config(address: &str) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: address.to_string(),
        }]
    }

    #[test]
    fn native_options_are_checked_without_a_runtime_or_reachable_server() {
        for address in [
            "mongodb://unresolvable.invalid/events?appName=Nervix",
            "mongodb://one.invalid:27017,two.invalid:27018/events?replicaSet=cluster",
        ] {
            let parsed = MongoDbClientConfiguration::parse(&config(address))
                .assured("native address validation needs neither DNS nor a live server");
            assert_eq!(parsed.database(), "events");
        }
        let report = validate_client_configuration(&config(
            "mongodb://unresolvable.invalid/events?maxConnecting=0",
        ))
        .expect_err("the native driver rejects zero concurrent connection establishment");
        assert!(report.downcast_ref::<mongodb::error::Error>().is_some());
        assert_eq!(report.to_string(), "invalid client config key 'addr'");
    }

    #[test]
    fn database_override_and_native_pool_refusals_are_explicit() {
        let mut settings = config("mongodb://unresolvable.invalid/original");
        settings.push(ClientConfigEntry {
            key: "database".to_string(),
            value: "Events".to_string(),
        });
        assert_eq!(
            MongoDbClientConfiguration::parse(&settings)
                .assured("the explicit database is valid")
                .database(),
            "Events"
        );
        for key in ["maxPoolSize", "minPoolSize"] {
            let report = validate_client_configuration(&config(&format!(
                "mongodb://unresolvable.invalid/events?{key}=2"
            )))
            .expect_err("a pool is sized only by POOL SIZE");
            assert!(
                matches!(report.current_context(), OfflineClientConfigError::PoolSizing { key: actual } if actual.as_str() == key)
            );
        }
    }

    #[test]
    fn a_configuration_resolves_one_valid_database() {
        let address = "mongodb://unresolvable.invalid";
        let missing = validate_client_configuration(&config(address))
            .expect_err("a client needs an explicit database or a URL database");
        assert_eq!(missing.to_string(), "invalid client config key 'database'");
        for database in ["".to_string(), "has space".to_string(), "é".repeat(32)] {
            let mut settings = config(address);
            settings.push(ClientConfigEntry {
                key: "database".to_string(),
                value: database,
            });
            let refusal = validate_client_configuration(&settings)
                .expect_err("the database must satisfy the native namespace contract");
            assert_eq!(refusal.to_string(), "invalid client config key 'database'");
        }
        let refusal =
            validate_client_configuration(&config("mongodb://unresolvable.invalid/Events%202026"))
                .expect_err("a percent-decoded database must satisfy the same contract");
        assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
    }

    #[test]
    fn identity_keys_are_paired_and_validated_without_reading_files() {
        let mut settings = config("mongodb://unresolvable.invalid/events?tls=true");
        for (key, value) in [
            ("tls_cert_file", "unavailable-cert.pem"),
            ("tls_key_file", "unavailable-key.pem"),
        ] {
            settings.push(ClientConfigEntry {
                key: key.to_owned(),
                value: value.to_owned(),
            });
        }
        validate_client_configuration(&settings)
            .assured("complete identity declaration needs neither files nor a server");
        settings[0].value = "mongodb://unresolvable.invalid/events?tls=false".to_owned();
        let report = validate_client_configuration(&settings)
            .expect_err("explicit identity files cannot override disabled TLS");
        assert!(matches!(
            report.downcast_ref::<crate::tls_identity::MongoDbTlsError>(),
            Some(crate::tls_identity::MongoDbTlsError::Disabled)
        ));
        assert_eq!(report.to_string(), "invalid client config key 'addr'");
        settings.pop();
        let report = validate_client_configuration(&settings)
            .expect_err("the key must accompany the certificate");
        assert!(
            report
                .downcast_ref::<nervix_models::ClientConfigSchemaError>()
                .is_some()
        );
    }
}
