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

use error_stack::Report;
use nervix_connector::OfflineClientConfigError;
use nervix_models::ClientConfigEntry;
use rdkafka::{config::ClientConfig, error::KafkaError};

/// Build and release only librdkafka's configuration object. No consumer or producer is created,
/// so no native transport thread, DNS lookup, broker call or consumer lifetime is opened.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    let mut driver = ClientConfig::new();
    for entry in config {
        driver.set(&entry.key, &entry.value);
    }
    match driver.create_native_config() {
        Ok(configuration) => {
            drop(configuration);
            Ok(())
        }
        Err(error) => {
            let context = match &error {
                KafkaError::ClientConfig(_, _, key, _) => {
                    OfflineClientConfigError::key(key.clone())
                }
                _ => OfflineClientConfigError::Configuration,
            };
            Err(Report::new(error).change_context(context))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_properties_are_checked_by_librdkafka_without_a_broker() {
        let config = |key: &str, value: &str| {
            [ClientConfigEntry {
                key: key.to_string(),
                value: value.to_string(),
            }]
        };
        validate_client_configuration(&config("fetch.wait.max.ms", "500"))
            .expect("a native property is accepted offline");
        for (key, value) in [
            ("nervix.unknown", "value"),
            ("fetch.wait.max.ms", "not-a-number"),
            ("auto.offset.reset", "not-an-offset"),
        ] {
            let refusal = validate_client_configuration(&config(key, value))
                .expect_err("librdkafka refuses this property");
            assert!(
                matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key: refused } if refused == key)
            );
            assert!(refusal.downcast_ref::<KafkaError>().is_some());
            assert_eq!(
                refusal.to_string(),
                format!("invalid client config key '{key}'")
            );
        }
    }
}
