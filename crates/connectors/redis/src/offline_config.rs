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
use nervix_connector::{
    OfflineClientConfigError, client_config_value, optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ConnectorKind};

/// Parse the current address without opening a connection or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::Redis.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    redis::Client::open(address.as_str()).change_context(OfflineClientConfigError::key("addr"))?;
    if optional_client_config_value(config, "subscription_buffer_messages").is_some() {
        crate::RedisPubSubIngestorPlan::buffer_messages(config).change_context(
            OfflineClientConfigError::key("subscription_buffer_messages"),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_subscription_capacity_uses_the_ingestor_plan_range() {
        let address = ClientConfigEntry {
            key: "addr".to_string(),
            value: "redis://127.0.0.1:6379/0".to_string(),
        };
        for value in ["1", "1024"] {
            validate_client_configuration(&[
                address.clone(),
                ClientConfigEntry {
                    key: "subscription_buffer_messages".to_string(),
                    value: value.to_string(),
                },
            ])
            .expect("a bounded subscription capacity is valid offline");
        }
        let refusal = validate_client_configuration(&[
            address,
            ClientConfigEntry {
                key: "subscription_buffer_messages".to_string(),
                value: u64::MAX.to_string(),
            },
        ])
        .expect_err("the channel cannot represent this subscription capacity");
        assert!(matches!(
            refusal.current_context(),
            OfflineClientConfigError::InvalidKey { key } if key == "subscription_buffer_messages"
        ));
    }

    #[test]
    fn offline_address_check_uses_the_driver_without_a_server() {
        let config = |value: &str| {
            [ClientConfigEntry {
                key: "addr".to_string(),
                value: value.to_string(),
            }]
        };
        validate_client_configuration(&config("redis://user:password@127.0.0.1:6379/0"))
            .expect("a driver-valid URL needs no running server");
        let refusal = validate_client_configuration(&config("http://127.0.0.1:6379/0"))
            .expect_err("the driver refuses this scheme");
        assert!(
            matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "addr")
        );
        assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
    }
}
