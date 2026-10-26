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
use nervix_connector::{OfflineClientConfigError, client_config_value};
use nervix_models::{ClientConfigEntry, ConnectorKind};

/// Parse the current address without opening a connection or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::RabbitMq.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    // amq-protocol-uri exposes its refusal as String, with no typed driver error available.
    address.parse::<lapin::uri::AMQPUri>().map_err(|reason| {
        error_stack::Report::new(OfflineClientConfigError::key("addr")).attach_printable(reason)
    })?;
    Ok(())
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
        validate_client_configuration(&config("amqp://user:password@127.0.0.1:5672/%2f"))
            .expect("a driver-valid URL needs no running server");
        let refusal = validate_client_configuration(&config("http://127.0.0.1:5672/%2f"))
            .expect_err("the driver refuses this scheme");
        assert!(
            matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "addr")
        );
        assert_eq!(refusal.to_string(), "invalid client config key 'addr'");
    }
}
