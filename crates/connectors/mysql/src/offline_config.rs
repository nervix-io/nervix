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

use error_stack::{Report, ResultExt as _};
use nervix_connector::{OfflineClientConfigError, client_config_value};
use nervix_models::{ClientConfigEntry, ClientPooling, ConnectorKind};
use url::Url;

/// Parse the current address without opening a connection or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::MySql.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    let url = Url::parse(&address).change_context(OfflineClientConfigError::key("addr"))?;
    for (key, _) in url.query_pairs() {
        if ClientPooling::Pooled.reserves_config_key(&key) {
            return Err(Report::new(OfflineClientConfigError::PoolSizing {
                key: key.into_owned(),
            }));
        }
    }
    mysql_async::Opts::from_url(&address).change_context(OfflineClientConfigError::key("addr"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_pool_overrides_are_refused_by_the_shared_declaration_without_values() {
        for key in ClientPooling::Pooled.reserved_config_keys() {
            let config = [ClientConfigEntry {
                key: "addr".to_string(),
                value: format!("mysql://user:private-password@127.0.0.1/database?{key}=9876"),
            }];
            let error =
                validate_client_configuration(&config).expect_err("POOL SIZE owns capacity");
            assert!(
                matches!(error.current_context(), OfflineClientConfigError::PoolSizing { key: found } if found == key)
            );
            let message = format!("{error:?}");
            assert!(message.contains("POOL SIZE"));
            assert!(!message.contains("private-password"));
            assert!(!message.contains("9876"));
        }
        for (parameter, decoded) in [("pool%5Fmax", "pool_max"), ("POOL_MIN", "POOL_MIN")] {
            let config = [ClientConfigEntry {
                key: "addr".to_string(),
                value: format!("mysql://127.0.0.1/database?{parameter}=1"),
            }];
            let error =
                validate_client_configuration(&config).expect_err("decoded pool keys are reserved");
            assert!(
                matches!(error.current_context(), OfflineClientConfigError::PoolSizing { key } if key == decoded)
            );
        }
    }

    #[test]
    fn offline_address_check_uses_the_driver_without_a_server() {
        let config = |value: &str| {
            [ClientConfigEntry {
                key: "addr".to_string(),
                value: value.to_string(),
            }]
        };
        validate_client_configuration(&config("mysql://user:password@127.0.0.1:3306/database"))
            .expect("a driver-valid URL needs no running server");
        let refusal = validate_client_configuration(&config("http://127.0.0.1:3306/database"))
            .expect_err("the driver refuses this scheme");
        assert!(
            matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "addr")
        );
        assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
    }
}
