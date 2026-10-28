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

use crate::address::RabbitMqAddress;

/// Read the address as every connection reads it, without opening one or resolving its host.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let address = client_config_value(config, "addr", ConnectorKind::RabbitMq.keyword())
        .change_context(OfflineClientConfigError::key("addr"))?;
    RabbitMqAddress::parse(&address).change_context(OfflineClientConfigError::key("addr"))?;
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

    #[test]
    fn addresses_that_name_no_amqp_broker_are_refused_before_commit() {
        let config = |value: &str| {
            [ClientConfigEntry {
                key: "addr".to_string(),
                value: value.to_string(),
            }]
        };
        for (address, case) in [
            (
                "amqp:/%2f",
                "no host, which the driver would read as localhost",
            ),
            ("amqp:///%2f", "an empty host"),
            ("amqp://guest:secret@127.0.0.1:0/%2f", "port zero"),
            (
                "amqps://guest:secret@[::1]:0/%2f",
                "port zero on an IPv6 host",
            ),
            (
                "http://guest:secret@127.0.0.1:5672/%2f",
                "a scheme that is not AMQP",
            ),
            ("amqp:guest:secret@127.0.0.1:5672", "no authority"),
            (
                "amqp://guest:secret@127.0.0.1:5672/%2f?heartbeat=soon",
                "a query option the driver cannot read",
            ),
            (
                "amqp://guest:secret@127.0.0.1:5672/%2f?auth_mechanism=kerberos",
                "an authentication mechanism the driver does not offer",
            ),
        ] {
            let Err(refusal) = validate_client_configuration(&config(address)) else {
                panic!("{case} must be refused before commit");
            };
            assert!(
                matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key } if key == "addr"),
                "{case}: {refusal:?}"
            );
            let reported = format!("{refusal:?}");
            assert!(
                !reported.contains("secret"),
                "{case} repeats its credentials: {reported}"
            );
        }
        for address in [
            "amqp://127.0.0.1",
            "amqp://rabbitmq.nervix.test:5672/%2f",
            "amqps://user:pass@[::1]:5671/orders?heartbeat=10&frame_max=8192&channel_max=16&\
             connection_timeout=1000&auth_mechanism=external",
        ] {
            if let Err(refusal) = validate_client_configuration(&config(address)) {
                panic!("{address} names a broker: {refusal:?}");
            }
        }
    }
}
