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

use meticulous::ResultExt as _;

use super::*;
use crate::{ClientConfigEntry, ConnectorKind, CreateClientHttp, DeadlineKey};

fn entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn an_exclusion_group_accepts_absence_or_one_key_and_refuses_every_pair() {
    let schema = ConnectorKind::Nats
        .client()
        .assured("NATS declares its client")
        .config;
    let address = entry("addr", "nats://127.0.0.1:4222");
    schema
        .validate(std::slice::from_ref(&address))
        .assured("authentication may be absent");
    for key in ["credentials_file", "nkey_seed", "token"] {
        schema
            .validate(&[address.clone(), entry(key, "secret")])
            .assured("one declared authentication method is accepted");
    }
    for (first, second) in [
        ("credentials_file", "nkey_seed"),
        ("credentials_file", "token"),
        ("nkey_seed", "token"),
    ] {
        for validate in [
            ClientConfigSchema::validate,
            ClientConfigSchema::validate_rendered,
        ] {
            let report = validate(
                schema,
                &[
                    address.clone(),
                    entry(second, "second-secret"),
                    entry(first, "first-secret"),
                ],
            )
            .expect_err("the exclusion is independent of entry order and rendering stage");
            assert_eq!(
                report.current_context(),
                &ClientConfigSchemaError::MutuallyExclusive { first, second }
            );
            assert_eq!(
                report.to_string(),
                format!("client config keys '{first}' and '{second}' are mutually exclusive")
            );
        }
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

#[test]
fn millisecond_lifetimes_keep_zero_and_whole_second_rules_distinct() {
    let idle = ClientConfigKey::optional(
        "pool_idle_timeout_ms",
        ClientConfigValue::NonnegativeMilliseconds,
    );
    for value in ["0", "00", "1", "1250"] {
        idle.validate(value)
            .expect("zero and positive schedulable millisecond lifetimes are valid");
    }
    let maximum = ClientConfigKey::optional(
        "pool_max_lifetime_ms",
        ClientConfigValue::WholeSecondsMilliseconds,
    );
    for value in ["1000", "30000", "60000"] {
        maximum
            .validate(value)
            .expect("positive whole seconds are exactly representable");
    }
    for value in ["+0", "-0", "1.5", "18446744073709551615"] {
        assert!(
            idle.validate(value).is_err(),
            "invalid idle lifetime {value}"
        );
    }
    for value in [
        "0",
        "999",
        "1001",
        "-1000",
        "1000.0",
        "18446744073709551615",
    ] {
        assert!(
            maximum.validate(value).is_err(),
            "invalid maximum lifetime {value}"
        );
    }
}

#[test]
fn a_level_template_admits_exactly_its_levels() {
    let key = ClientConfigKey::optional("namespace", ClientConfigValue::Levels("tenant/namespace"));
    assert_eq!(key.value.label(), "tenant/namespace");
    for value in ["public/default", "tenant-a/orders", "t/n", "tenant:a/ns=eu"] {
        key.validate(value)
            .assured("two nonempty levels without whitespace match the template");
    }
    for value in [
        "",
        "tenant",
        "tenant/",
        "/namespace",
        "tenant//namespace",
        "tenant/namespace/topic",
        "ten ant/namespace",
        "tenant/name\u{a0}space",
        "tenant/namespace\n",
        "tenant/\u{7}",
    ] {
        let report = key
            .validate(value)
            .expect_err("a value that does not name exactly the template's levels is refused");
        assert_eq!(
            report.current_context(),
            &ClientConfigSchemaError::InvalidValue {
                key: "namespace".to_string(),
                expected: "tenant/namespace",
            },
            "{value:?} does not name a tenant and a namespace"
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
        .validate(&[entry("addr", "mqtt://broker.example.com:1883"), property])
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

const OWNED_MODE: OwnedQueryOption = OwnedQueryOption {
    name: "mode",
    aliases: &["mode-alias"],
    choices: &["plain", "verified"],
};

const OWNED_ADDRESS: ClientConfigValue = ClientConfigValue::DriverUrl(DriverUrl {
    schemes: &["db", "dbs"],
    owned: &[OWNED_MODE],
});

const OWNED_KEYS: [ClientConfigKey; 2] = [
    ClientConfigKey::required("addr", OWNED_ADDRESS),
    ClientConfigKey::TLS_CA.only_for_query("addr", "mode", "verified"),
];

const OWNED_URL: ClientConfigSchema = ClientConfigSchema::new(&OWNED_KEYS, &[], &[]);

const ALIASED_KEYS: [ClientConfigKey; 1] =
    [ClientConfigKey::optional("accepted.name", ClientConfigValue::String).driver()];
const ALIASES: [ClientConfigAlias; 1] = [ClientConfigAlias {
    alias: "other.name",
    accepted: "accepted.name",
}];
const SCOPE: ClientConfigStrippedPrefix = ClientConfigStrippedPrefix {
    prefix: "scope.",
    properties: &["scope.own"],
};
const ALIASED: ClientConfigSchema = ClientConfigSchema::new(
    &ALIASED_KEYS,
    &[ClientConfigNamespace::All],
    &["owned.name"],
)
.with_driver_aliases(&ALIASES, Some(SCOPE));

#[test]
fn a_driver_alias_is_refused_alone_or_beside_its_accepted_name_at_both_stages() {
    let alone = [entry("other.name", "value")];
    let beside = [
        entry("accepted.name", "value"),
        entry("other.name", "value"),
    ];
    for refusal in [
        ALIASED.validate(&alone),
        ALIASED.validate_rendered(&alone),
        ALIASED.validate(&beside),
        ALIASED.validate_rendered(&beside),
    ] {
        let refusal = refusal.expect_err("the driver reads one property under both names");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::DriverAlias {
                key: "other.name".to_string(),
                accepted: "accepted.name",
            }
        );
        assert_eq!(
            refusal.to_string(),
            "client config key 'other.name' is the driver's other name for 'accepted.name'; write \
             'accepted.name'"
        );
    }
    let accepted = [entry("accepted.name", "value")];
    ALIASED
        .validate(&accepted)
        .assured("the accepted name passes to the driver");
    ALIASED
        .validate_rendered(&accepted)
        .assured("the accepted name passes to the driver");
    assert_eq!(ALIASED.aliases(), &ALIASES);
}

#[test]
fn a_key_under_a_stripped_prefix_is_refused_except_the_drivers_own_properties() {
    for key in ["scope.accepted.name", "scope.other.name", "scope.unknown"] {
        let refusal = ALIASED
            .validate(&[entry(key, "value")])
            .expect_err("the driver reads a key under the prefix as another property's name");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::DriverPrefix {
                key: key.to_string(),
                prefix: "scope.",
            }
        );
        assert_eq!(
            refusal.to_string(),
            format!(
                "client config key '{key}' is written under the driver's 'scope.' prefix; write \
                 it without the prefix"
            )
        );
    }
    for key in ["scope.own", "unscoped.scope.name", "scope"] {
        ALIASED
            .validate_rendered(&[entry(key, "value")])
            .assured("a driver property of its own or a key outside the prefix passes");
    }
    assert_eq!(ALIASED.stripped_prefix(), Some(SCOPE));
}

#[test]
fn a_schema_without_driver_aliases_refuses_none() {
    assert!(OWNED_URL.aliases().is_empty());
    assert_eq!(OWNED_URL.stripped_prefix(), None);
    assert_eq!(
        ClientConfigSchema::new(
            &ALIASED_KEYS,
            &[ClientConfigNamespace::All],
            &["owned.name"]
        )
        .with_driver_aliases(&ALIASES, Some(SCOPE)),
        ALIASED
    );
}

#[test]
fn a_stripped_prefix_admits_its_own_properties_declared_as_keys() {
    const OWN: ClientConfigStrippedPrefix = ClientConfigStrippedPrefix {
        prefix: "scope.",
        properties: &["scope.first", "scope.second"],
    };
    const KEYS: [ClientConfigKey; 2] = [
        ClientConfigKey::optional("id", ClientConfigValue::String),
        ClientConfigKey::optional("scope.first", ClientConfigValue::String),
    ];
    let schema = ClientConfigSchema::new(&KEYS, &[], &[]).with_driver_aliases(&[], Some(OWN));
    assert_eq!(schema.stripped_prefix(), Some(OWN));
    schema
        .validate(&[entry("id", "value"), entry("scope.first", "value")])
        .assured("a key shorter than the prefix and a declared property of its own pass");
    let refusal = schema
        .validate(&[entry("scope.third", "value")])
        .expect_err("the driver strips the prefix from a key that names none of its properties");
    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::DriverPrefix {
            key: "scope.third".to_string(),
            prefix: "scope.",
        }
    );
}

#[test]
#[should_panic(expected = "name_precedes")]
fn a_stripped_prefix_lists_its_own_properties_sorted() {
    const UNSORTED: ClientConfigStrippedPrefix = ClientConfigStrippedPrefix {
        prefix: "scope.",
        properties: &["scope.second", "scope.first"],
    };
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_driver_aliases(&[], Some(UNSORTED));
}

#[test]
#[should_panic(expected = "holds_name")]
fn a_refused_name_is_no_reserved_key() {
    const RESERVED: [ClientConfigAlias; 1] = [ClientConfigAlias {
        alias: "owned.name",
        accepted: "accepted.name",
    }];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &["owned.name"])
        .with_driver_aliases(&RESERVED, None);
}

#[test]
fn a_reserved_key_a_statement_sets_names_the_clause_that_sets_it() {
    const STATEMENT_KEYS: [ClientConfigStatementKey; 1] = [ClientConfigStatementKey {
        key: "owned.start",
        clause: "START AT on the ingestor",
    }];
    let schema = ClientConfigSchema::new(&ALIASED_KEYS, &[], &["owned.group", "owned.start"])
        .with_statement_keys(&STATEMENT_KEYS);

    let refusal = schema
        .validate(&[entry("owned.start", "value")])
        .expect_err("a statement sets this key");
    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::SetByStatement {
            key: "owned.start".to_string(),
            clause: "START AT on the ingestor",
        }
    );

    let refusal = schema
        .validate(&[entry("owned.group", "value")])
        .expect_err("Nervix sets this key itself");
    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::Reserved {
            key: "owned.group".to_string()
        }
    );
    assert_eq!(schema.statement_keys(), &STATEMENT_KEYS);
}

#[test]
#[should_panic(expected = "holds_name")]
fn a_key_a_statement_sets_is_reserved() {
    const UNRESERVED: [ClientConfigStatementKey; 1] = [ClientConfigStatementKey {
        key: "owned.start",
        clause: "START AT on the ingestor",
    }];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_statement_keys(&UNRESERVED);
}

#[test]
#[should_panic(expected = "name_precedes")]
fn keys_a_statement_sets_are_sorted() {
    const UNSORTED: [ClientConfigStatementKey; 2] = [
        ClientConfigStatementKey {
            key: "owned.start",
            clause: "START AT on the ingestor",
        },
        ClientConfigStatementKey {
            key: "owned.group",
            clause: "OFFSET BY on the ingestor",
        },
    ];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &["owned.group", "owned.start"])
        .with_statement_keys(&UNSORTED);
}

#[test]
#[should_panic(expected = "name_precedes")]
fn driver_aliases_are_sorted_by_the_name_each_refuses() {
    const UNSORTED: [ClientConfigAlias; 2] = [
        ClientConfigAlias {
            alias: "z.name",
            accepted: "accepted.name",
        },
        ClientConfigAlias {
            alias: "other.name",
            accepted: "accepted.name",
        },
    ];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_driver_aliases(&UNSORTED, None);
}

#[test]
#[should_panic(expected = "declares")]
fn a_refused_name_is_no_declared_key() {
    const DECLARED: [ClientConfigAlias; 1] = [ClientConfigAlias {
        alias: "accepted.name",
        accepted: "third.name",
    }];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_driver_aliases(&DECLARED, None);
}

#[test]
#[should_panic(expected = "refuses_alias")]
fn an_accepted_name_is_no_name_another_pair_refuses() {
    const CHAINED: [ClientConfigAlias; 2] = [
        ClientConfigAlias {
            alias: "first.name",
            accepted: "second.name",
        },
        ClientConfigAlias {
            alias: "second.name",
            accepted: "accepted.name",
        },
    ];
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_driver_aliases(&CHAINED, None);
}

#[test]
#[should_panic(expected = "begins_with")]
fn a_stripped_prefix_names_only_its_own_properties_under_it() {
    const ELSEWHERE: ClientConfigStrippedPrefix = ClientConfigStrippedPrefix {
        prefix: "scope.",
        properties: &["unscoped.own"],
    };
    ClientConfigSchema::new(&ALIASED_KEYS, &[], &[]).with_driver_aliases(&[], Some(ELSEWHERE));
}

#[test]
fn a_schema_is_built_only_when_its_address_offers_each_conditioning_choice() {
    assert_eq!(ClientConfigSchema::new(&OWNED_KEYS, &[], &[]), OWNED_URL);
}

#[test]
#[should_panic(expected = "owns_choice")]
fn a_schema_refuses_a_key_conditioned_on_a_choice_its_address_does_not_offer() {
    const UNOFFERED: [ClientConfigKey; 2] = [
        ClientConfigKey::required("addr", OWNED_ADDRESS),
        ClientConfigKey::TLS_CA.only_for_query("addr", "mode", "unverified"),
    ];
    ClientConfigSchema::new(&UNOFFERED, &[], &[]);
}

#[test]
fn an_owned_query_option_is_set_once_to_a_declared_choice_by_any_spelling() {
    for (address, choice) in [
        ("db://host/x?mode=plain", "plain"),
        (
            "dbs://host/x?other=1&mode-alias=verified&driver=2",
            "verified",
        ),
    ] {
        let parsed = Url::parse(address).assured("a fixture URL");
        assert_eq!(
            OWNED_MODE
                .value_in("addr", &parsed)
                .assured("one declared choice"),
            choice
        );
        OWNED_URL
            .validate(&[entry("addr", address)])
            .assured("the driver reads the other options");
    }
    for address in [
        "db://host/x",
        "db://host/x?mode=other",
        "db://host/x?mode=Plain",
        "db://host/x?mode=plain&mode=plain",
        "db://host/x?mode=plain&mode-alias=verified",
    ] {
        let report = OWNED_URL
            .validate(&[entry("addr", address)])
            .expect_err("Nervix owns the option");
        assert_eq!(
            report.current_context(),
            &ClientConfigSchemaError::QueryOption {
                key: "addr".to_string(),
                option: "mode",
                choices: &["plain", "verified"],
            },
            "{address}"
        );
    }
    let report = OWNED_URL
        .validate(&[entry("addr", "http://host/x?mode=plain")])
        .expect_err("Nervix owns the scheme");
    assert_eq!(
        report.current_context(),
        &ClientConfigSchemaError::Scheme {
            key: "addr".to_string(),
            schemes: &["db", "dbs"],
        }
    );
    assert_eq!(
        OWNED_URL.ownership_label(),
        "Nervix; address query delegated to driver"
    );
}

#[test]
fn a_key_conditioned_on_an_owned_choice_requires_its_address_to_set_it() {
    for validate in [
        ClientConfigSchema::validate,
        ClientConfigSchema::validate_rendered,
    ] {
        validate(
            OWNED_URL,
            &[
                entry("addr", "db://host/x?mode=verified"),
                entry("tls_ca_file", "authority.pem"),
            ],
        )
        .assured("the declared choice admits the key");
        let report = validate(
            OWNED_URL,
            &[
                entry("addr", "db://host/x?mode=plain"),
                entry("tls_ca_file", "authority.pem"),
            ],
        )
        .expect_err("the key requires the verified choice");
        assert_eq!(
            report.current_context(),
            &ClientConfigSchemaError::RequiresQuery {
                key: "tls_ca_file",
                address: "addr",
                option: "mode",
                value: "verified",
            }
        );
    }
    OWNED_URL
        .validate(&[
            entry("addr", "{{ address }}"),
            entry("tls_ca_file", "authority.pem"),
        ])
        .assured("a templated address is held to the choice once rendered");
    let inspected = OWNED_URL.inspection_entries(&[entry("addr", "db://host/x?mode=plain")]);
    assert_eq!(inspected, vec![entry("addr", CLIENT_CONFIG_REDACTION)]);

    const OPTIONAL_ADDRESS: [ClientConfigKey; 2] = [
        ClientConfigKey::optional("addr", OWNED_ADDRESS),
        ClientConfigKey::TLS_CA.only_for_query("addr", "mode", "verified"),
    ];
    let report = ClientConfigSchema::new(&OPTIONAL_ADDRESS, &[], &[])
        .validate(&[entry("tls_ca_file", "authority.pem")])
        .expect_err("without its address the key has no choice to rely on");
    assert!(matches!(
        report.current_context(),
        ClientConfigSchemaError::RequiresQuery {
            key: "tls_ca_file",
            ..
        }
    ));
}

#[test]
fn a_key_is_read_by_every_entity_of_its_client_unless_declared_for_one_side() {
    let shared = ClientConfigKey::optional("addr", ClientConfigValue::String);
    assert_eq!(shared.readers, ClientConfigReaders::Every);
    assert!(shared.readers.read_on(ClientConfigSide::Ingestor));
    assert!(shared.readers.read_on(ClientConfigSide::Emitter));

    let intake = ClientConfigKey::IDLE_TIMEOUT.read_by_ingestors();
    assert_eq!(intake.readers, ClientConfigReaders::Ingestors);
    assert!(intake.readers.read_on(ClientConfigSide::Ingestor));
    assert!(!intake.readers.read_on(ClientConfigSide::Emitter));

    let publication = ClientConfigKey::TIMEOUT.read_by_emitters();
    assert_eq!(publication.readers, ClientConfigReaders::Emitters);
    assert!(!publication.readers.read_on(ClientConfigSide::Ingestor));
    assert!(publication.readers.read_on(ClientConfigSide::Emitter));

    // The side that reads a key changes nothing else the key declares.
    assert_eq!(
        ClientConfigKey {
            readers: ClientConfigReaders::Every,
            ..intake
        },
        ClientConfigKey::IDLE_TIMEOUT
    );
    assert_eq!(
        ClientConfigKey {
            readers: ClientConfigReaders::Every,
            ..publication
        },
        ClientConfigKey::TIMEOUT
    );
}

const SIDED_SCHEMA: ClientConfigSchema = ClientConfigSchema::new(
    &[
        ClientConfigKey::TIMEOUT.read_by_emitters(),
        ClientConfigKey::optional("transport", ClientConfigValue::Choice(&["plain", "secure"])),
        ClientConfigKey::optional("window", ClientConfigValue::PositiveInteger).read_by_ingestors(),
    ],
    &[ClientConfigNamespace::Prefix("driver.")],
    &[],
);

#[test]
fn a_side_refuses_a_key_only_the_other_side_reads() {
    let intake = [entry("transport", "plain"), entry("window", "4")];
    SIDED_SCHEMA
        .validate_side(&intake, ClientConfigSide::Ingestor)
        .expect("ingestors read every key of this client");
    let refused = SIDED_SCHEMA
        .validate_side(&intake, ClientConfigSide::Emitter)
        .expect_err("emitters do not read the window");
    assert_eq!(
        refused.current_context(),
        &ClientConfigSchemaError::ReadByOtherSide {
            key: "window",
            readers: ClientConfigReaders::Ingestors,
        }
    );
    assert_eq!(
        refused.current_context().to_string(),
        "client config key 'window' is read only by ingestors"
    );

    let publication = [entry("timeout_ms", "1000")];
    SIDED_SCHEMA
        .validate_side(&publication, ClientConfigSide::Emitter)
        .expect("emitters read the timeout");
    let refused = SIDED_SCHEMA
        .validate_side(&publication, ClientConfigSide::Ingestor)
        .expect_err("ingestors do not read the timeout");
    assert_eq!(
        refused.current_context().to_string(),
        "client config key 'timeout_ms' is read only by emitters"
    );
}

#[test]
fn a_side_leaves_keys_it_does_not_judge_to_the_schema() {
    let entries = [entry("driver.anything", "1"), entry("undeclared", "1")];
    for side in [ClientConfigSide::Ingestor, ClientConfigSide::Emitter] {
        SIDED_SCHEMA
            .validate_side(&entries, side)
            .expect("delegated and undeclared keys are not a side's to refuse");
    }
}

const CHOICE_SCHEMA: ClientConfigSchema = ClientConfigSchema::new(
    &[
        ClientConfigKey::TLS_CA.only_for_choice("transport", &["secure", "mutual"]),
        ClientConfigKey::required(
            "transport",
            ClientConfigValue::Choice(&["plain", "secure", "mutual"]),
        ),
    ],
    &[],
    &[],
);

#[test]
fn a_key_conditioned_on_a_choice_needs_its_selector_to_select_it() {
    for selected in ["secure", "mutual"] {
        CHOICE_SCHEMA
            .validate(&[entry("transport", selected), entry("tls_ca_file", "/ca")])
            .expect("the selector selects a value the key is declared for");
    }
    CHOICE_SCHEMA
        .validate(&[entry("transport", "plain")])
        .expect("the condition binds only a key that is written");
    let refused = CHOICE_SCHEMA
        .validate(&[entry("transport", "plain"), entry("tls_ca_file", "/ca")])
        .expect_err("the selector selects another value");
    assert_eq!(
        refused.current_context(),
        &ClientConfigSchemaError::RequiresChoice {
            key: "tls_ca_file",
            selector: "transport",
            choices: &["secure", "mutual"],
        }
    );
    assert_eq!(
        refused.current_context().to_string(),
        "client config key 'tls_ca_file' requires 'transport' to be secure or mutual"
    );
}

/// A choice-conditioned key whose selector is optional, so the key can be written alone.
const OPTIONAL_SELECTOR_KEYS: [ClientConfigKey; 2] = [
    ClientConfigKey::TLS_CA.only_for_choice("transport", &["secure"]),
    ClientConfigKey::optional("transport", ClientConfigValue::Choice(&["plain", "secure"])),
];

#[test]
fn a_key_conditioned_on_a_choice_is_refused_without_its_selector() {
    // Built at run time, so the declaration's construction checks run here too.
    let schema = ClientConfigSchema::new(&OPTIONAL_SELECTOR_KEYS, &[], &[]);
    schema
        .validate(&[])
        .expect("a condition binds only a key that is written");
    let refused = schema
        .validate(&[entry("tls_ca_file", "/ca")])
        .expect_err("the key's selector is not written");
    assert_eq!(
        refused.current_context(),
        &ClientConfigSchemaError::RequiresChoice {
            key: "tls_ca_file",
            selector: "transport",
            choices: &["secure"],
        }
    );
}

#[test]
#[should_panic(expected = "selects_choices")]
fn a_schema_refuses_a_key_conditioned_on_a_choice_its_selector_does_not_offer() {
    const UNOFFERED: [ClientConfigKey; 2] = [
        ClientConfigKey::TLS_CA.only_for_choice("transport", &["mutual"]),
        ClientConfigKey::optional("transport", ClientConfigValue::Choice(&["plain", "secure"])),
    ];
    ClientConfigSchema::new(&UNOFFERED, &[], &[]);
}

#[test]
fn a_templated_selector_waits_for_its_rendered_value() {
    let declared = [
        entry("transport", "{{ selected }}"),
        entry("tls_ca_file", "/ca"),
    ];
    CHOICE_SCHEMA
        .validate(&declared)
        .expect("a declared template is judged once it is rendered");
    let refused = CHOICE_SCHEMA
        .validate_rendered(&declared)
        .expect_err("a rendered value must be a declared choice");
    assert!(matches!(
        refused.current_context(),
        ClientConfigSchemaError::InvalidValue { key, .. } if key == "transport"
    ));
}

#[test]
fn a_host_port_value_holds_a_host_and_a_port_and_nothing_else() {
    let key = ClientConfigKey::required("addr", ClientConfigValue::HostPort);
    for address in ["127.0.0.1:514", "[::1]:514", "syslog.example.test:514"] {
        key.validate(address).expect("the value is host:port");
    }
    for address in ["missing-port", "syslog.example.test:514/path", ""] {
        let refused = key
            .validate(address)
            .expect_err("the value is not host:port");
        assert_eq!(
            refused.current_context(),
            &ClientConfigSchemaError::InvalidValue {
                key: "addr".to_string(),
                expected: "host:port",
            }
        );
    }
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
fn a_client_accepts_the_secret_keys_an_operator_names_and_stays_strict_about_every_other_key() {
    let client = ConnectorKind::Http
        .client()
        .assured("HTTP declares a client");
    let base = [
        entry("endpoint", "https://api.example.com"),
        entry("timeout_ms", "5000"),
    ];

    let mut authored = base.to_vec();
    authored.push(entry("secret.api_token", "Bearer fixture-token"));
    authored.push(entry("secret.tenant_key", "fixture-tenant"));
    client
        .config
        .validate(&authored)
        .assured("a client names as many secret keys as it reads");
    assert_eq!(client.config.secret_key_prefix(), Some("secret."));
    assert_eq!(
        client.config.ownership_label(),
        "Nervix",
        "no driver reads a secret key, so the family delegates nothing"
    );
    assert!(client.config.namespaces().is_empty());

    for key in ["secret.", "secrets.api_token", "api_token", "secret"] {
        let mut refused = base.to_vec();
        refused.push(entry(key, "value"));
        let refusal = client
            .config
            .validate(&refused)
            .expect_err("only the prefix and a name make a secret key");
        assert!(
            matches!(
                refusal.current_context(),
                ClientConfigSchemaError::UnknownKey { key: found } if found == key
            ),
            "{key}: {refusal:?}"
        );
    }
    let mut repeated = base.to_vec();
    repeated.push(entry("secret.api_token", "first"));
    repeated.push(entry("secret.api_token", "second"));
    assert!(matches!(
        client
            .config
            .validate(&repeated)
            .expect_err("a secret key occurs once")
            .current_context(),
        ClientConfigSchemaError::DuplicateKey { .. }
    ));
    let mut uppercase = base.to_vec();
    uppercase.push(entry("secret.API_TOKEN", "value"));
    assert!(matches!(
        client
            .config
            .validate(&uppercase)
            .expect_err("a secret key is lowercase like every key")
            .current_context(),
        ClientConfigSchemaError::KeyCase { .. }
    ));
}

#[test]
fn inspection_masks_every_secret_key_and_a_schema_reports_which_keys_are_secret() {
    let client = ConnectorKind::Http
        .client()
        .assured("HTTP declares a client");
    let inspected = client.config.inspection_entries(&[
        entry("timeout_ms", "5000"),
        entry("secret.api_token", "Bearer fixture-token"),
    ]);
    assert_eq!(
        inspected,
        [
            entry("timeout_ms", "5000"),
            entry("secret.api_token", CLIENT_CONFIG_REDACTION),
        ]
    );

    assert!(client.config.is_secret("secret.api_token"));
    assert!(!client.config.is_secret("secret."));
    assert!(!client.config.is_secret("timeout_ms"));
    assert!(!client.config.is_secret("unknown"));

    let declared = ConnectorKind::ClickHouse
        .client()
        .assured("ClickHouse declares a client");
    assert_eq!(declared.config.secret_key_prefix(), None);
    assert!(declared.config.is_secret("password"));
    assert!(!declared.config.is_secret("secret.api_token"));
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

    /// The scheme a generated address key uses: the one a key bound to a scheme requires of it,
    /// otherwise any the key declares.
    fn address_scheme(
        arbitrary: &mut Arbitrary<'_>,
        address_schemes: &BTreeMap<&str, &'static str>,
        key: &str,
        schemes: &'static [&'static str],
    ) -> &'static str {
        if let Some(scheme) = address_schemes.get(key) {
            return scheme;
        }
        let index = arbitrary
            .entropy()
            .index(NonZeroUsize::new(schemes.len()).assured("a URL declares schemes"));
        schemes[index]
    }

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
            /// One Nervix-owned query option of one address key.
            #[derive(PartialEq, Eq, PartialOrd, Ord)]
            struct OwnedOptionOf {
                address: &'static str,
                option: &'static str,
            }
            let mut address_schemes = BTreeMap::new();
            let mut address_choices = BTreeMap::new();
            let mut conditioned_choices: BTreeMap<&str, Vec<&[&str]>> = BTreeMap::new();
            for key in schema.keys() {
                match key.presence {
                    // Every key is generated, so a key conditioned on a choice narrows that
                    // choice to what every such key permits.
                    ClientConfigPresence::OptionalForChoice { selector, choices } => {
                        conditioned_choices
                            .entry(selector)
                            .or_default()
                            .push(choices);
                    }
                    // The generator writes every key, so a key bound to a scheme decides the scheme
                    // its address uses.
                    ClientConfigPresence::OptionalForScheme { address, scheme }
                    | ClientConfigPresence::ExactlyForScheme { address, scheme } => {
                        address_schemes.insert(address, scheme);
                    }
                    // Every key is generated, so a key conditioned on an owned option's choice
                    // fixes that choice in its address.
                    ClientConfigPresence::OptionalForQuery {
                        address,
                        option,
                        value,
                    } => {
                        address_choices.insert(OwnedOptionOf { address, option }, value);
                    }
                    _ => {}
                }
            }
            let mut entries = Vec::new();
            let mut selected_exclusive = None;
            if let Some(keys) = schema.mutually_exclusive_keys()
                && arbitrary.entropy().flag()
            {
                let index = arbitrary.entropy().index(
                    NonZeroUsize::new(keys.len())
                        .assured("a declared exclusion group names at least two keys"),
                );
                selected_exclusive = Some(keys[index]);
            }
            for key in schema.keys() {
                if let Some(keys) = schema.mutually_exclusive_keys()
                    && keys.binary_search(&key.name).is_ok()
                    && selected_exclusive != Some(key.name)
                {
                    continue;
                }
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
                    ClientConfigValue::NonnegativeMilliseconds => {
                        arbitrary.entropy().boundary_biased(0..=60_000).to_string()
                    }
                    ClientConfigValue::WholeSecondsMilliseconds => {
                        Duration::from_secs(arbitrary.entropy().boundary_biased(1..=60))
                            .as_millis()
                            .to_string()
                    }
                    ClientConfigValue::TcpKeepaliveMilliseconds => {
                        Duration::from_secs(arbitrary.entropy().boundary_biased(1..=32_767))
                            .as_millis()
                            .to_string()
                    }
                    ClientConfigValue::Url(schemes) => {
                        let scheme =
                            address_scheme(&mut arbitrary, &address_schemes, key.name, schemes);
                        format!("{scheme}://user:password@example.com/api")
                    }
                    ClientConfigValue::HostPortUrl(schemes) => {
                        let scheme =
                            address_scheme(&mut arbitrary, &address_schemes, key.name, schemes);
                        let host =
                            arbitrary
                                .entropy()
                                .pick(["example.com", "127.0.0.1", "[2001:db8::1]"]);
                        let port = arbitrary.entropy().boundary_biased(1..=u64::from(u16::MAX));
                        let root = if arbitrary.entropy().flag() { "/" } else { "" };
                        format!("{scheme}://{host}:{port}{root}")
                    }
                    ClientConfigValue::DriverAddress => match address_schemes.get(key.name) {
                        Some(scheme) => format!("{scheme}://user:password@example.com/0"),
                        None => "redis://user:password@example.com/0".to_string(),
                    },
                    ClientConfigValue::DriverUrl(declared) => {
                        let scheme = address_scheme(
                            &mut arbitrary,
                            &address_schemes,
                            key.name,
                            declared.schemes,
                        );
                        let mut url = format!(
                            "{scheme}://user:password@example.com/db?application_name=fixture"
                        );
                        for option in declared.owned {
                            let owned_option = OwnedOptionOf {
                                address: key.name,
                                option: option.name,
                            };
                            let choice = match address_choices.get(&owned_option) {
                                Some(choice) => *choice,
                                None => {
                                    let index = arbitrary.entropy().index(
                                        NonZeroUsize::new(option.choices.len())
                                            .assured("an owned option declares choices"),
                                    );
                                    option.choices[index]
                                }
                            };
                            url.push_str(&format!("&{}={choice}", option.name));
                        }
                        url
                    }
                    ClientConfigValue::HostPort => {
                        let host = arbitrary.entropy().pick([
                            "collector.example.test",
                            "127.0.0.1",
                            "[2001:db8::1]",
                        ]);
                        let port = arbitrary.entropy().boundary_biased(0..=u64::from(u16::MAX));
                        format!("{host}:{port}")
                    }
                    ClientConfigValue::Choice(choices) => {
                        let conditions = conditioned_choices.get(key.name);
                        let mut permitted = Vec::new();
                        for choice in choices {
                            let held = match conditions {
                                Some(conditions) => {
                                    conditions.iter().all(|allowed| allowed.contains(choice))
                                }
                                None => true,
                            };
                            if held {
                                permitted.push(*choice);
                            }
                        }
                        let index = arbitrary.entropy().index(
                            NonZeroUsize::new(permitted.len())
                                .assured("the keys a schema conditions on a choice agree on one"),
                        );
                        permitted[index].to_string()
                    }
                    ClientConfigValue::File(_) => {
                        format!("/mount/{}.pem", arbitrary.entropy().byte())
                    }
                    ClientConfigValue::Levels(template) => {
                        let mut levels = Vec::new();
                        for name in template.split('/') {
                            levels.push(format!("{name}{}", arbitrary.entropy().byte()));
                        }
                        levels.join("/")
                    }
                };
                entries.push(entry(key.name, &value));
                assert_eq!(schema.key(key.name), Some(key));
            }
            if let Some(prefix) = schema.secret_key_prefix() {
                entries.push(entry(
                    &format!("{prefix}fixture_secret"),
                    &arbitrary.string(),
                ));
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
