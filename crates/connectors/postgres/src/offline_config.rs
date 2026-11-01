//! Network-free validation of the configuration the host is preparing to commit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The driver's offline configuration refusal, with its complete typed cause.
//! - **Depends on.** The connector contract, configuration entries and this connector's driver.
//! - **Must not know.** Models, transaction admission, resource resolution or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the connector driver validation boundary")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies the client's resolved entries")
)]

use error_stack::ResultExt as _;
use nervix_connector::{ClientConfigError, OfflineClientConfigError, client_config_value};
use nervix_models::{ClientConfigEntry, ConnectorKind};

/// Parse the current address without opening a connection or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::Postgres.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    // Keep sqlx's typed URL refusal beneath the public key context before applying the
    // connector's stricter TLS policy through its existing options builder.
    address
        .parse::<sqlx::postgres::PgConnectOptions>()
        .change_context(OfflineClientConfigError::key("addr"))?;
    match crate::table::connect_options(&address, config) {
        Ok(_) => Ok(()),
        Err(report) => {
            let key = match report.downcast_ref::<ClientConfigError>() {
                Some(ClientConfigError::InvalidDeadline { key }) => *key,
                _ => "addr",
            };
            Err(report.change_context(OfflineClientConfigError::key(key)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_address_check_uses_the_driver_without_a_server() {
        let config = |value: &str| {
            [ClientConfigEntry {
                key: "addr".to_string(),
                value: value.to_string(),
            }]
        };
        validate_client_configuration(&config(
            "postgres://user:password@127.0.0.1:5432/database?sslmode=disable",
        ))
        .expect("a driver-valid URL needs no running server");
        let refusal = validate_client_configuration(&config("http://127.0.0.1:5432/database"))
            .expect_err("the driver refuses this scheme");
        assert!(
            matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "addr")
        );
        assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
    }

    #[test]
    fn offline_statement_deadline_refusal_names_its_key_and_keeps_its_cause() {
        let config = [
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "postgres://127.0.0.1/nervix?sslmode=disable".to_string(),
            },
            ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "2147483648".to_string(),
            },
        ];
        let report = validate_client_configuration(&config)
            .expect_err("the deadline exceeds PostgreSQL's statement_timeout range");
        assert!(
            matches!(report.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "timeout_ms")
        );
        assert!(report.contains::<ClientConfigError>());
        assert!(report.contains::<std::num::TryFromIntError>());
    }
}
