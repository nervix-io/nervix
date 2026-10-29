//! Schema validation, complete configuration round trips and confidential inspection properties.
//!
//! Layer: test harness.
//! - **Owns.** Bounded evidence for the current client declaration boundary.
//! - **Depends on.** Client vocabulary, its encodings and bounded generators.
//! - **Must not know.** Drivers, network services or runtime resource installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns declaration conformance evidence")
)]

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;
use crate::{ClientConfigEntry, ConnectorKind, CreateClientHttp, DeadlineKey};

fn entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn defaulted_keys_preserve_authored_configuration_and_explicit_values() {
    let schema = ConnectorKind::ClickHouse
        .client()
        .assured("ClickHouse declares a configured client")
        .config;
    let authored = vec![
        entry("addr", "http://localhost:8123"),
        entry("timeout_ms", "5000"),
    ];
    let effective = schema.effective_entries(&authored);
    assert_eq!(
        effective,
        [
            authored[0].clone(),
            authored[1].clone(),
            entry("pool_idle_timeout_ms", "2000"),
            entry("tcp_keepalive_ms", "60000"),
        ]
    );
    assert_eq!(schema.effective_entries(&effective), effective);
    assert_eq!(authored.len(), 2);
    schema
        .validate_rendered(&effective)
        .assured("declared defaults are valid");

    for value in ["1", "2000", "10000"] {
        let mut explicit = authored.clone();
        explicit.push(entry("pool_idle_timeout_ms", value));
        let projected = schema.effective_entries(&explicit);
        assert_eq!(&projected[..explicit.len()], explicit);
        assert_eq!(projected.last(), Some(&entry("tcp_keepalive_ms", "60000")));
    }
    let mut invalid = authored;
    invalid.push(entry("pool_idle_timeout_ms", "0"));
    assert!(
        schema
            .validate_rendered(&schema.effective_entries(&invalid))
            .is_err()
    );
}

#[test]
fn tcp_keepalive_values_fit_the_socket_option_without_truncation() {
    let key = ClientConfigKey::optional(
        "tcp_keepalive_ms",
        ClientConfigValue::TcpKeepaliveMilliseconds,
    );
    for value in ["1000", "60000", "32767000"] {
        key.validate(value)
            .assured("whole seconds fit Linux TCP_KEEPIDLE");
    }
    for value in [
        "0", "1", "999", "1001", "32768000", "-1000", "1000.0", "invalid",
    ] {
        let report = key
            .validate(value)
            .expect_err("the socket option cannot represent this interval");
        assert!(
            matches!(report.current_context(), ClientConfigSchemaError::InvalidValue { key, .. } if key == "tcp_keepalive_ms")
        );
    }
}

fn mqtt() -> ClientConfigSchema {
    ConnectorKind::Mqtt
        .client()
        .assured("MQTT declares a configured client")
        .config
}

#[test]
fn sentry_checks_the_request_deadline_its_http_transport_reads() {
    let schema = ConnectorKind::Sentry
        .client()
        .assured("Sentry declares a configured client")
        .config;
    let dsn = entry("dsn", "http://public@127.0.0.1:8000/1");
    schema
        .validate(&[dsn.clone(), entry("timeout_ms", "1500")])
        .assured("Sentry accepts a bounded request deadline before transport initialization");
    let refusal = schema
        .validate(&[dsn.clone(), entry("timeout_ms", "0")])
        .expect_err("a request deadline must be positive");
    assert!(matches!(
        refusal.current_context(),
        ClientConfigSchemaError::InvalidValue { key, .. } if key == "timeout_ms"
    ));
    let Err(missing) = schema.validate(&[dsn]) else {
        panic!("the request deadline is required");
    };
    assert!(matches!(
        missing.current_context(),
        ClientConfigSchemaError::MissingRequired { key } if *key == "timeout_ms"
    ));
}

#[test]
fn a_strict_schema_accepts_exactly_its_declared_keys_and_required_values() {
    let address = entry("addr", "mqtt://broker.example.com:1883");
    mqtt()
        .validate(&[address.clone(), entry("client_id", "worker")])
        .assured("both MQTT keys are declared with string values");

    let report = mqtt()
        .validate(&[address.clone(), entry("transport_option", "value")])
        .expect_err("an undeclared key cannot be committed");
    assert!(matches!(
        report.current_context(),
        ClientConfigSchemaError::UnknownKey { key } if key == "transport_option"
    ));

    let report = mqtt()
        .validate(&[address.clone(), address])
        .expect_err("a configuration cannot give one key two values");
    assert!(matches!(
        report.current_context(),
        ClientConfigSchemaError::DuplicateKey { key } if key == "addr"
    ));

    let report = mqtt()
        .validate(&[entry("client_id", "worker")])
        .expect_err("the client requires its broker address");
    assert!(matches!(
        report.current_context(),
        ClientConfigSchemaError::MissingRequired { key } if *key == "addr"
    ));

    let report = mqtt()
        .validate(&[entry("ADDR", "mqtt://broker.example.com:1883")])
        .expect_err("configuration keys are lowercase and case-sensitive");
    assert!(
        matches!(report.current_context(), ClientConfigSchemaError::KeyCase { key } if key == "ADDR")
    );
}

#[test]
fn delegated_properties_keep_their_namespace_and_reserved_key_boundary() {
    let schema = ConnectorKind::Kafka
        .client()
        .assured("Kafka declares a client")
        .config;
    let brokers = entry("bootstrap.servers", "broker-1:9092");
    let property = entry("topic.metadata.refresh.interval.ms", "1000");
    let entries = [brokers.clone(), property.clone()];
    schema
        .validate(&entries)
        .assured("librdkafka owns this property namespace");
    assert_eq!(
        schema.inspection_entries(&entries),
        vec![brokers, entry(&property.key, CLIENT_CONFIG_REDACTION)]
    );
    let refusal = mqtt()
        .validate(&[entry("addr", "mqtt://broker.example.com"), property])
        .expect_err("MQTT declares no delegated property namespace");
    assert!(
        matches!(refusal.current_context(), ClientConfigSchemaError::UnknownKey { key } if key == "topic.metadata.refresh.interval.ms")
    );
    for key in [
        "allow.auto.create.topics",
        "delivery.report.only.error",
        "enable.auto.commit",
        "enable.partition.eof",
        "group.id",
    ] {
        let refusal = schema
            .validate(&[entry(key, "value")])
            .expect_err("Nervix owns this consumer setting");
        assert!(
            matches!(refusal.current_context(), ClientConfigSchemaError::Reserved { key: refused } if refused == key)
        );
    }
}

#[test]
fn a_declared_boolean_is_checked_without_exposing_its_value() {
    let schema = ConnectorKind::ZeroMq
        .client()
        .assured("ZeroMQ declares a configured client")
        .config;
    let address = entry("addr", "tcp://127.0.0.1:5555");
    for value in ["true", "false", "TRUE", "FALSE"] {
        schema
            .validate(&[address.clone(), entry("bind", value)])
            .assured("the declared boolean accepts the connector's boolean spellings");
    }
    let report = schema
        .validate(&[address, entry("bind", "private-invalid-value")])
        .expect_err("an arbitrary string is not a boolean");
    assert!(matches!(
        report.current_context(),
        ClientConfigSchemaError::InvalidValue { key, .. } if key == "bind"
    ));
    assert!(!format!("{report:#}").contains("private-invalid-value"));
}

#[test]
fn a_tls_identity_requires_its_matching_key_in_both_directions() {
    let address = entry("addr", "mqtts://broker.example.com:8883");
    for (present, required) in [
        ("tls_cert_file", "tls_key_file"),
        ("tls_key_file", "tls_cert_file"),
    ] {
        let report = mqtt()
            .validate(&[address.clone(), entry(present, "identity.pem")])
            .expect_err("one half of a TLS identity cannot configure the client");
        assert!(matches!(
            report.current_context(),
            ClientConfigSchemaError::RequiresKey { key, required: needed }
                if *key == present && *needed == required
        ));
    }
    mqtt()
        .validate(&[
            address,
            entry("tls_ca_file", "authority.pem"),
            entry("tls_cert_file", "identity.pem"),
            entry("tls_key_file", "identity.key"),
        ])
        .assured("the complete identity satisfies the declared pair");
}

#[test]
fn a_tls_address_requires_its_declared_trust_file() {
    let address = entry("addr", "mqtts://broker.example.com:8883");
    for validate in [
        ClientConfigSchema::validate,
        ClientConfigSchema::validate_rendered,
    ] {
        let report = validate(mqtt(), std::slice::from_ref(&address))
            .expect_err("MQTT TLS requires its explicit trust file before commit");
        assert!(matches!(
            report.current_context(),
            ClientConfigSchemaError::RequiresKey {
                key: "addr",
                required: "tls_ca_file"
            }
        ));
        validate(
            mqtt(),
            &[address.clone(), entry("tls_ca_file", "authority.pem")],
        )
        .assured("the declared trust file satisfies MQTT TLS");
    }
    mqtt()
        .validate(&[entry("addr", "mqtt://broker.example.com:1883")])
        .assured("the plaintext scheme requires no trust file");
    mqtt()
        .validate(&[entry("addr", "{{broker_address}}")])
        .assured("the rendered address determines its trust requirement before commit");
    ConnectorKind::Http
        .client()
        .assured("HTTP declares a client")
        .config
        .validate(&[
            entry("endpoint", "https://api.example.com"),
            entry("timeout_ms", "5000"),
        ])
        .assured("HTTPS uses its driver's built-in trust roots");
}

#[test]
fn a_deadline_uses_the_shared_positive_millisecond_contract() {
    let schema = ConnectorKind::Http
        .client()
        .assured("HTTP declares a configured client")
        .config;
    for value in ["1", "5000", "0005000"] {
        schema
            .validate(&[
                entry("endpoint", "http://api.example.com"),
                entry("timeout_ms", value),
            ])
            .assured("each value is a schedulable positive millisecond count");
    }
    for value in ["0", "-1", "1.5", "5s", "18446744073709551616"] {
        let report = schema
            .validate(&[
                entry("endpoint", "http://api.example.com"),
                entry("timeout_ms", value),
            ])
            .expect_err("the shared deadline contract refuses this count");
        assert!(matches!(
            report.current_context(),
            ClientConfigSchemaError::InvalidValue { key, .. } if key == "timeout_ms"
        ));
    }
}

#[test]
fn http_declares_one_strict_client_transport_contract() {
    let client = ConnectorKind::Http
        .client()
        .assured("HTTP declares a client");
    let keys: Vec<_> = client.config.keys().iter().map(|key| key.name).collect();
    assert_eq!(
        keys,
        [
            "endpoint",
            "follow_redirects",
            "proxy",
            "timeout_ms",
            "tls_ca_file",
            "tls_cert_file",
            "tls_key_file",
        ]
    );
    client
        .config
        .validate(&[
            entry("endpoint", "https://api.example.com"),
            entry("follow_redirects", "true"),
            entry("proxy", "http://proxy.example.com:8080"),
            entry("timeout_ms", "5000"),
        ])
        .assured("the declared HTTP transport settings are valid together");
    for (key, value) in [
        ("follow_redirects", "yes"),
        ("proxy", "socks5://proxy.example.com:1080"),
    ] {
        let refusal = client
            .config
            .validate(&[
                entry("endpoint", "https://api.example.com"),
                entry("timeout_ms", "5000"),
                entry(key, value),
            ])
            .expect_err("the transport option has its declared value type");
        assert!(matches!(
            refusal.current_context(),
            ClientConfigSchemaError::InvalidValue { key: found, .. } if found == key
        ));
    }
    let missing = client
        .config
        .validate(&[entry("endpoint", "https://api.example.com")])
        .expect_err("the HTTP timeout is required");
    assert!(matches!(
        missing.current_context(),
        ClientConfigSchemaError::MissingRequired { key: "timeout_ms" }
    ));
    let contract = CreateClientHttp::TRANSPORT;
    assert_eq!(contract.timeout, DeadlineKey::Timeout);
    assert_eq!(contract.redirect_limit_without_opt_in, 0);
    assert_eq!(contract.redirect_limit_with_opt_in, 10);
    assert!(!contract.reads_environment_proxy);
    assert!(contract.redacts_targets_and_headers);
    assert!(contract.tls_identity_requires_pair);
}

#[test]
fn cloud_endpoint_inspection_hides_credentials_and_query_options() {
    let schema = ConnectorKind::S3
        .client()
        .assured("S3 declares its object-store endpoint")
        .config;
    for value in [
        "https://user:private-password@account.blob.example.com",
        "https://account.blob.example.com/?sig=private-signature",
    ] {
        let complete = [entry("endpoint", value)];
        assert_eq!(
            schema.inspection_entries(&complete),
            vec![entry("endpoint", CLIENT_CONFIG_REDACTION)]
        );
        assert_eq!(complete[0].value, value);
    }
    let public = [entry("endpoint", "https://account.blob.example.com")];
    assert_eq!(schema.inspection_entries(&public), public);
}

#[test]
fn secret_metadata_controls_inspection_and_preserves_the_complete_entries() {
    let schema = ConnectorKind::Sqs
        .client()
        .assured("SQS declares a configured client")
        .config;
    let complete = vec![
        entry("endpoint", "http://queue.example.com"),
        entry("region", "us-east-1"),
        entry("secret_access_key", "private-credential"),
    ];
    let inspected = schema.inspection_entries(&complete);
    assert_eq!(
        inspected,
        vec![
            complete[0].clone(),
            complete[1].clone(),
            entry("secret_access_key", "********"),
        ]
    );
    assert_eq!(complete[2].value, "private-credential");
}

#[test]
fn bolero_schema_checked_client_entries_round_trip() {
    use std::{collections::BTreeMap, num::NonZeroUsize};

    use nervix_arbitrary::{Arbitrary, Domain};
    use strum::IntoEnumIterator as _;

    let schemas: Vec<_> = ConnectorKind::iter()
        .filter_map(|kind| kind.client())
        .map(|client| client.config)
        .collect();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
            let index = arbitrary
                .entropy()
                .index(NonZeroUsize::new(schemas.len()).assured("clients declare schemas"));
            let schema = schemas[index];
            let mut address_schemes = BTreeMap::new();
            for key in schema.keys() {
                if let ClientConfigPresence::OptionalForScheme { address, scheme } = key.presence {
                    address_schemes.insert(address, scheme);
                }
            }
            let mut entries = Vec::new();
            for key in schema.keys() {
                if matches!(key.presence, ClientConfigPresence::Defaulted(_))
                    && arbitrary.entropy().flag()
                {
                    continue;
                }
                let value = match key.value {
                    ClientConfigValue::String => arbitrary.string(),
                    ClientConfigValue::PercentEncodedHeaders => format!(
                        "x-fixture={}",
                        percent_encoding::utf8_percent_encode(
                            &arbitrary.string(),
                            percent_encoding::NON_ALPHANUMERIC
                        )
                    ),
                    ClientConfigValue::Boolean => arbitrary.entropy().flag().to_string(),
                    ClientConfigValue::UnsignedInteger => arbitrary
                        .entropy()
                        .boundary_biased(0..=u64::MAX)
                        .to_string(),
                    ClientConfigValue::PositiveInteger => arbitrary
                        .entropy()
                        .boundary_biased(1..=u64::MAX)
                        .to_string(),
                    ClientConfigValue::Milliseconds => {
                        arbitrary.entropy().boundary_biased(1..=60_000).to_string()
                    }
                    ClientConfigValue::TcpKeepaliveMilliseconds => {
                        Duration::from_secs(arbitrary.entropy().boundary_biased(1..=32_767))
                            .as_millis()
                            .to_string()
                    }
                    ClientConfigValue::Url(schemes) => {
                        let scheme = match address_schemes.get(key.name) {
                            Some(scheme) => *scheme,
                            None => {
                                let index = arbitrary.entropy().index(
                                    NonZeroUsize::new(schemes.len())
                                        .assured("a URL declares schemes"),
                                );
                                schemes[index]
                            }
                        };
                        format!("{scheme}://user:password@example.com/api")
                    }
                    ClientConfigValue::DriverAddress => {
                        "redis://user:password@example.com/0".to_string()
                    }
                    ClientConfigValue::Choice(choices) => {
                        let index = arbitrary.entropy().index(
                            NonZeroUsize::new(choices.len()).assured("a choice declares values"),
                        );
                        choices[index].to_string()
                    }
                    ClientConfigValue::File(_) => {
                        format!("/mount/{}.pem", arbitrary.entropy().byte())
                    }
                };
                entries.push(entry(key.name, &value));
                assert_eq!(schema.key(key.name), Some(key));
            }
            if let Some(count) = NonZeroUsize::new(schema.namespaces().len()) {
                let namespace = schema.namespaces()[arbitrary.entropy().index(count)];
                let prefix = match namespace {
                    ClientConfigNamespace::All => "driver.",
                    ClientConfigNamespace::Prefix(prefix) => prefix,
                };
                entries.push(entry(
                    &format!("{prefix}fixture_property"),
                    &arbitrary.string(),
                ));
            }
            schema
                .validate_rendered(&entries)
                .assured("the generator stays in the declaration's value domain");
            let json = serde_json::to_vec(&entries).assured("configuration entries serialize");
            let decoded: Vec<ClientConfigEntry> =
                serde_json::from_slice(&json).assured("serialized entries decode");
            assert_eq!(decoded, entries);
            let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&entries)
                .assured("configuration entries archive");
            let decoded =
                rkyv::from_bytes::<Vec<ClientConfigEntry>, rkyv::rancor::Error>(&archived)
                    .assured("archived entries decode");
            assert_eq!(decoded, entries);
            schema
                .validate_rendered(&decoded)
                .assured("round trips preserve the schema's accepted value");
            let inspected = schema.inspection_entries(&decoded);
            assert_eq!(schema.inspection_entries(&inspected), inspected);
            let effective = schema.effective_entries(&decoded);
            assert_eq!(&effective[..decoded.len()], decoded);
            assert_eq!(schema.effective_entries(&effective), effective);
            schema
                .validate_rendered(&effective)
                .assured("default projection retains schema acceptance");
            let inspected = schema.inspection_entries(&effective);
            assert_eq!(schema.inspection_entries(&inspected), inspected);
        });
}
