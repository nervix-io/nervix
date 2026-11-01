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
    use std::collections::BTreeMap;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::ConnectorKind;

    use super::*;

    /// What librdkafka reads for `read` once only `name` is set to `value`.
    fn native_value(name: &str, value: &str, read: &str) -> String {
        let mut driver = ClientConfig::new();
        driver.set(name, value);
        let native = driver
            .create_native_config()
            .assured("librdkafka accepts each sample value of the declared names");
        native
            .get(read)
            .assured("librdkafka reads each declared name once a value is set under it")
    }

    #[test]
    fn declared_driver_names_follow_the_linked_librdkafka() {
        let (_, version) = rdkafka::util::get_rdkafka_version();
        assert_eq!(
            version, "2.12.1",
            "the Kafka client declares the names librdkafka 2.12.1 reads; read them again from \
             the property table of the linked librdkafka"
        );
        let schema = ConnectorKind::Kafka
            .client()
            .assured("Kafka declares its client")
            .config;
        // A value librdkafka accepts under each accepted name, other than its default.
        let samples = BTreeMap::from([
            ("acks", "1"),
            ("bootstrap.servers", "broker-1:9092"),
            ("compression.type", "lz4"),
            ("delivery.timeout.ms", "2500"),
            ("linger.ms", "25"),
            ("max.in.flight.requests.per.connection", "7"),
            ("max.partition.fetch.bytes", "4096"),
            ("retries", "7"),
            ("sasl.mechanism", "SCRAM-SHA-512"),
            (
                "sasl.oauthbearer.client.credentials.client.id",
                "nervix-client",
            ),
            (
                "sasl.oauthbearer.client.credentials.client.secret",
                "nervix-secret",
            ),
        ]);
        assert!(!schema.aliases().is_empty());
        for alias in schema.aliases() {
            let value = samples
                .get(alias.accepted)
                .assured("every accepted name has a sample value");
            let direct = native_value(alias.accepted, value, alias.accepted);
            let through_alias = native_value(alias.alias, value, alias.accepted);
            assert_eq!(
                through_alias, direct,
                "librdkafka reads '{}' as the property of '{}'",
                alias.alias, alias.accepted
            );
            assert_eq!(native_value(alias.alias, value, alias.alias), direct);
        }

        let stripped = schema
            .stripped_prefix()
            .assured("the Kafka client declares librdkafka's topic prefix");
        assert_eq!(stripped.prefix, "topic.");
        // A default-topic property is read under the prefix.
        assert_eq!(
            native_value("topic.request.timeout.ms", "4321", "request.timeout.ms"),
            "4321"
        );
        // Each declared exception is accepted as written while the rest of its name is no
        // property, so librdkafka reads it as a global property rather than stripping it.
        let exception_samples = BTreeMap::from([
            ("topic.blacklist", "internal\\..*"),
            ("topic.metadata.propagation.max.ms", "1000"),
            ("topic.metadata.refresh.fast.cnt", "5"),
            ("topic.metadata.refresh.fast.interval.ms", "100"),
            ("topic.metadata.refresh.interval.ms", "1000"),
            ("topic.metadata.refresh.sparse", "false"),
        ]);
        assert_eq!(stripped.properties.len(), exception_samples.len());
        for property in stripped.properties {
            let value = exception_samples
                .get(property)
                .assured("every declared exception has a sample value");
            let entry = |key: &str| ClientConfigEntry {
                key: key.to_string(),
                value: (*value).to_string(),
            };
            validate_client_configuration(&[entry(property)])
                .expect("librdkafka reads the exception as one of its own properties");
            let unprefixed = property
                .strip_prefix(stripped.prefix)
                .assured("each exception is written under the prefix");
            let refusal = validate_client_configuration(&[entry(unprefixed)])
                .expect_err("the rest of an exception's name is no librdkafka property");
            assert!(
                matches!(refusal.current_context(), OfflineClientConfigError::InvalidKey { key: refused } if refused == unprefixed),
                "librdkafka refuses '{unprefixed}' as an unknown property"
            );
        }
    }

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
