//! Tests of the MQTT vocabulary's executable topic contract.
//!
//! Layer: test harness.
//! - **Owns.** MQTT declaration, topic filter, publication topic and client-configuration contract
//!   checks through the public vocabulary.
//! - **Depends on.** MQTT Models, entity kinds and shared declaration types.
//! - **Must not know.** The MQTT driver, parsing, planning or runtime subscriptions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;
use strum::IntoEnumIterator as _;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigFile, ClientConfigOwner, ClientConfigPresence,
    ClientConfigSchema, ClientConfigSchemaError, ClientConfigValue, ClientPooling,
    EmitterEntityKind, EmitterPublishingModeKind, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityKind, IngestorEntityModel as _, WrittenChoice,
};

#[test]
fn topic_ingestion_acknowledges_only_through_a_session_the_broker_keeps() {
    let ingest = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic).capabilities();
    let kept_session = [
        ChoiceRequirement {
            option: "SESSION",
            choices: &["PERSISTENT"],
        },
        ChoiceRequirement {
            option: "QOS",
            choices: &["1", "2"],
        },
    ];

    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Mqtt)
    );
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::NoAck),
        Some(&DeclaredDelivery::NO_ACK)
    );
    for kind in [
        IngestDeliveryKind::AckSequential,
        IngestDeliveryKind::AckParallel,
    ] {
        let delivery = ingest
            .declared_delivery(kind)
            .assured("both acknowledged deliveries are declared");
        assert_eq!(delivery.requires, kept_session, "{kind:?}");
    }
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::AckRequest),
        None
    );
    assert_eq!(ingest.position_class, PositionClass::BrokerRedelivered);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::Written
    );

    let suspend = ingest
        .declared_quiesce(IngestQuiesceKind::Suspend)
        .assured("a kept session lets the source suspend");
    assert_eq!(suspend.requires, kept_session);
    assert_eq!(
        ingest.declared_quiesce(IngestQuiesceKind::Buffer),
        Some(&DeclaredQuiesce::BUFFER)
    );
    assert_eq!(
        ingest.declared_quiesce(IngestQuiesceKind::Drop),
        Some(&DeclaredQuiesce::DROP)
    );
    assert_eq!(ingest.declared_quiesce(IngestQuiesceKind::Reject), None);
}

#[test]
fn topic_ingestion_subscribes_a_filter_with_a_required_session_and_quality_of_service() {
    let ingest = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic).capabilities();

    assert_eq!(
        ingest.locator,
        Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(MqttTopicFilter::DECLARATION),
        })
    );
    let keywords: Vec<&str> = ingest
        .options
        .iter()
        .map(|option| option.keywords)
        .collect();
    assert_eq!(keywords, vec!["INSTANCES", "SESSION", "QOS"]);
    assert_eq!(
        ingest.option("INSTANCES"),
        Some(&OptionDeclaration::INSTANCES)
    );

    let session = ingest
        .option("SESSION")
        .assured("the topic ingestor declares its session option");
    assert_eq!(session.presence, OptionPresence::Required);
    assert_eq!(
        session.examples(),
        vec!["SESSION CLEAN", "SESSION PERSISTENT"]
    );

    let qos = ingest
        .option("QOS")
        .assured("the topic ingestor declares its quality-of-service option");
    assert_eq!(qos.presence, OptionPresence::Required);
    assert_eq!(qos.examples(), vec!["QOS 0", "QOS 1", "QOS 2"]);
    assert_eq!(qos.choice("3"), None);
}

#[rstest]
#[case::clean_at_most_once(MqttSession::Clean, MqttQos::Qos0)]
#[case::clean_at_least_once(MqttSession::Clean, MqttQos::Qos1)]
#[case::clean_exactly_once(MqttSession::Clean, MqttQos::Qos2)]
#[case::kept_at_most_once(MqttSession::Persistent, MqttQos::Qos0)]
#[case::kept_at_least_once(MqttSession::Persistent, MqttQos::Qos1)]
#[case::kept_exactly_once(MqttSession::Persistent, MqttQos::Qos2)]
fn an_unacknowledged_source_reports_the_session_and_quality_of_service_it_wrote(
    #[case] session: MqttSession,
    #[case] qos: MqttQos,
) {
    let source = topic_source(session, qos, MqttIngestMode::NoAck);

    assert_eq!(source.chosen("SESSION"), Some(session.keyword()));
    assert_eq!(source.chosen("QOS"), Some(qos.spelling()));
    assert_eq!(source.chosen("INSTANCES"), None);
    assert_eq!(
        source.acknowledgement(),
        IngestAcknowledgement::Unacknowledged
    );
    assert!(!source.mode.is_ack());
    assert_eq!(source.mode.acknowledgement_window(), None);
    assert_eq!(source.unmet_delivery(), None);
}

#[test]
fn the_quality_of_service_levels_spell_what_mqtt_numbers_them() {
    let mut spelled = Vec::new();
    for qos in MqttQos::iter() {
        spelled.push((qos.level(), qos.spelling()));
    }
    assert_eq!(spelled, vec![(0, "0"), (1, "1"), (2, "2")]);
}

fn acknowledged_modes() -> [MqttIngestMode; 2] {
    let retry_policy = RetryPolicy {
        backoff: "1s".to_string(),
        max_backoff: "30s".to_string(),
    };
    [
        MqttIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: retry_policy.clone(),
        },
        MqttIngestMode::AckParallel {
            max: NonZeroU64::new(8).assured("eight is nonzero"),
            batch_timeout: "100ms".to_string(),
            timeout: "5s".to_string(),
            retry_policy,
        },
    ]
}

#[rstest]
#[case::at_least_once(MqttQos::Qos1)]
#[case::exactly_once(MqttQos::Qos2)]
fn an_acknowledged_source_subscribes_through_a_persistent_session_at_qos_one_or_two(
    #[case] qos: MqttQos,
) {
    let [sequential, parallel] =
        acknowledged_modes().map(|mode| topic_source(MqttSession::Persistent, qos, mode));

    for source in [&sequential, &parallel] {
        assert_eq!(source.chosen("SESSION"), Some("PERSISTENT"));
        assert_eq!(source.chosen("QOS"), Some(qos.spelling()));
        assert!(source.mode.is_ack());
        assert_eq!(source.unmet_delivery(), None);
        assert!(source.supports_quiesce(&IngestQuiesceMode::Suspend));
    }
    assert_eq!(
        sequential.mode.acknowledgement_window(),
        Some(NonZeroU64::MIN)
    );
    assert_eq!(parallel.mode.acknowledgement_window(), NonZeroU64::new(8));
}

#[rstest]
#[case::clean_at_most_once(MqttSession::Clean, MqttQos::Qos0, "CLEAN", "0")]
#[case::clean_at_least_once(MqttSession::Clean, MqttQos::Qos1, "CLEAN", "1")]
#[case::clean_exactly_once(MqttSession::Clean, MqttQos::Qos2, "CLEAN", "2")]
#[case::kept_at_most_once(MqttSession::Persistent, MqttQos::Qos0, "PERSISTENT", "0")]
fn an_acknowledged_mode_outside_a_kept_session_names_both_written_values(
    #[case] session: MqttSession,
    #[case] qos: MqttQos,
    #[case] session_keyword: &'static str,
    #[case] qos_spelling: &'static str,
) {
    for mode in acknowledged_modes() {
        let source = topic_source(session, qos, mode);
        let delivery = source.acknowledgement().kind();
        let unmet = source
            .unmet_delivery()
            .assured("an acknowledged mode needs the persistent session at QoS 1 or 2");
        assert_eq!(unmet.entity, MqttTopicIngestor::KIND);
        assert_eq!(unmet.delivery, delivery);
        assert_eq!(unmet.requires, BROKER_KEPT_SESSION);
        assert_eq!(
            unmet.written,
            vec![
                WrittenChoice {
                    option: "SESSION",
                    choice: Some(session_keyword),
                },
                WrittenChoice {
                    option: "QOS",
                    choice: Some(qos_spelling),
                },
            ]
        );
        assert_eq!(
            unmet.to_string(),
            format!(
                "MQTT TOPIC ingestors name MODE {} only with SESSION PERSISTENT and QOS 1 or 2; \
                 the statement writes SESSION {session_keyword} and QOS {qos_spelling}",
                delivery.phrase()
            )
        );
        assert!(!source.supports_quiesce(&IngestQuiesceMode::Suspend));
    }
}

#[test]
fn an_acknowledged_mode_beside_options_a_statement_does_not_answer_names_them_unwritten() {
    let ingest = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic).capabilities();
    let unmet = ingest
        .unmet_delivery(
            MqttTopicIngestor::KIND,
            IngestDeliveryKind::AckSequential,
            &|_| None,
        )
        .assured("a statement that answers no option makes none of the choices ACK requires");

    assert_eq!(
        unmet.written,
        vec![
            WrittenChoice {
                option: "SESSION",
                choice: None,
            },
            WrittenChoice {
                option: "QOS",
                choice: None,
            },
        ]
    );
    assert_eq!(
        unmet.to_string(),
        "MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or \
         2; the statement writes no SESSION and no QOS"
    );
    assert_eq!(
        ingest.unmet_delivery(MqttTopicIngestor::KIND, IngestDeliveryKind::NoAck, &|_| {
            None
        }),
        None,
        "NO ACK requires no choice"
    );
}

fn topic_source(session: MqttSession, qos: MqttQos, mode: MqttIngestMode) -> MqttTopicIngestor {
    MqttTopicIngestor {
        client: ClientName::try_from("mqtt_main").assured("a lowercase identifier is a name"),
        topic: MqttTopicFilter::try_from("devices/+").assured("one wildcard level is a filter"),
        instances: NonZeroU64::MIN,
        session,
        qos,
        mode,
        quiesce: IngestQuiesceMode::Drop,
    }
}

#[test]
fn topic_emission_publishes_at_the_quality_of_service_its_mode_names() {
    let emit = EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic).capabilities();

    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Mqtt)
    );
    assert_eq!(
        emit.publishing_modes,
        &[
            EmitterPublishingModeKind::MqttQos0,
            EmitterPublishingModeKind::MqttQos1,
            EmitterPublishingModeKind::MqttQos2,
        ]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert_eq!(emit.batch, EmitterBatchLegality::Optional { maximum: None });
    assert!(!emit.requires_batch_transformation);
    assert_eq!(
        emit.locator,
        Some(LocatorDeclaration {
            keywords: "TOPIC",
            name: LocatorName::External(MqttTopicName::DECLARATION),
        })
    );
    assert!(emit.options.is_empty());
    assert!(emit.envelope.is_empty());
    assert!(emit.properties.is_empty());
}

#[test]
fn both_topic_directions_carry_no_transport_headers_or_typed_metadata() {
    let ingest = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic).capabilities();

    assert_eq!(ingest.headers, None);
    assert!(!ingest.reads_headers());
    assert!(ingest.metadata.fields().is_empty());
    assert_eq!(emit.headers, None);
}

#[test]
fn topic_ingestion_bounds_its_connection_by_the_keep_alive_but_not_its_suback() {
    let ingest = IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic).capabilities();

    let waits: Vec<WaitBound> = ingest.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        waits,
        vec![
            WaitBound::Driver("5 s"),
            WaitBound::Unbounded,
            WaitBound::Driver("one keep-alive interval of 60 s, or the broker's"),
            WaitBound::Driver("keep-alive of 60 s, or the broker's"),
        ]
    );
    assert!(ingest.bounds.deadline_keys().is_empty());
    let mut overflows = Vec::with_capacity(ingest.bounds.buffers.len());
    for buffer in ingest.bounds.buffers {
        let BufferBound::Bounded { overflow, .. } = buffer.bound else {
            panic!("every MQTT source buffer is bounded: {}", buffer.holds);
        };
        overflows.push(overflow);
    }
    assert_eq!(
        overflows,
        vec![
            BufferOverflow::PeerDiscards,
            BufferOverflow::DropAndCount,
            BufferOverflow::Backpressure,
            BufferOverflow::DropAndCount,
            BufferOverflow::Backpressure,
            BufferOverflow::Backpressure,
            BufferOverflow::Backpressure,
            BufferOverflow::Backpressure,
        ]
    );
    assert_eq!(
        ingest.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Bytes(MQTT_MAXIMUM_PACKET_BYTES),
            overflow: BufferOverflow::PeerDiscards,
        }
    );
    assert_eq!(
        ingest.bounds.buffers[1].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Bytes(MQTT_SOURCE_MAXIMUM_PACKET_BYTES),
            overflow: BufferOverflow::DropAndCount,
        }
    );
    assert_eq!(ingest.bounds.driver_limits.len(), 2);
}

#[test]
fn topic_emission_bounds_broker_answers_by_its_ack_timeout_clause() {
    let emit = EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic).capabilities();

    assert_eq!(emit.bounds.buffers.len(), 1);
    assert_eq!(
        emit.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("1 under QOS 0, ACK PARALLEL MAX under QOS 1 and 2"),
            overflow: BufferOverflow::FailPublish,
        }
    );
    let waits: Vec<WaitBound> = emit.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        waits,
        vec![
            WaitBound::Driver("5 s"),
            WaitBound::Clause("ACK TIMEOUT"),
            WaitBound::Unbounded,
        ]
    );
    assert!(emit.bounds.deadline_keys().is_empty());
    assert_eq!(
        emit.bounds.driver_limits,
        &["Packets above the broker's maximum packet size are rejected."]
    );
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn client_schema() -> ClientConfigSchema {
    CreateClientMqtt::<u64>::DECLARATION.config
}

#[test]
fn client_declaration_names_the_broker_and_binds_tls_files_to_mqtts() {
    let declaration = CreateClientMqtt::<u64>::DECLARATION;
    assert_eq!(CreateClientMqtt::<u64>::CONNECTOR, ConnectorKind::Mqtt);
    assert_eq!(declaration.pooling, ClientPooling::Unpooled);
    assert!(declaration.options.is_empty());

    let config = declaration.config;
    let names: Vec<&str> = config.keys().iter().map(|key| key.name).collect();
    assert_eq!(
        names,
        vec![
            "addr",
            "client_id",
            "tls_ca_file",
            "tls_cert_file",
            "tls_key_file"
        ]
    );
    for key in config.keys() {
        assert_eq!(key.owner, ClientConfigOwner::Nervix, "{}", key.name);
        assert!(!key.confidential, "{}", key.name);
    }
    let expected = [
        (
            "addr",
            ClientConfigPresence::Required,
            ClientConfigValue::HostPortUrl(&["mqtt", "mqtts"]),
        ),
        (
            "client_id",
            ClientConfigPresence::Optional,
            ClientConfigValue::String,
        ),
        (
            "tls_ca_file",
            ClientConfigPresence::ExactlyForScheme {
                address: "addr",
                scheme: "mqtts",
            },
            ClientConfigValue::File(ClientConfigFile::CertificateAuthority),
        ),
        (
            "tls_cert_file",
            ClientConfigPresence::OptionalForScheme {
                address: "addr",
                scheme: "mqtts",
            },
            ClientConfigValue::File(ClientConfigFile::Certificate),
        ),
        (
            "tls_key_file",
            ClientConfigPresence::OptionalForScheme {
                address: "addr",
                scheme: "mqtts",
            },
            ClientConfigValue::File(ClientConfigFile::PrivateKey),
        ),
    ];
    for (name, presence, value) in expected {
        let key = config
            .key(name)
            .assured("the fixture lists only keys the MQTT client declares");
        assert_eq!(key.presence, presence, "{name}");
        assert_eq!(key.value, value, "{name}");
    }
    assert_eq!(config.ownership_label(), "Nervix");
    assert!(config.namespaces().is_empty());
    assert!(config.reserved().is_empty());
}

#[rstest]
#[case::ipv4("mqtt://127.0.0.1:1883")]
#[case::domain_name("mqtt://broker.example.com:1883")]
#[case::ipv6("mqtt://[2001:db8::1]:1883")]
#[case::root_path("mqtt://broker.example.com:1883/")]
#[case::uppercase_scheme("MQTT://broker.example.com:1883")]
#[case::highest_port("mqtt://broker.example.com:65535")]
fn client_schema_accepts_an_address_that_names_a_broker(#[case] address: &str) {
    for validate in [
        ClientConfigSchema::validate,
        ClientConfigSchema::validate_rendered,
    ] {
        validate(
            client_schema(),
            &[
                config_entry("addr", address),
                config_entry("client_id", "Nervix Reader é"),
            ],
        )
        .assured("a plaintext address names its broker by host and port alone");
    }
}

#[test]
fn client_schema_accepts_a_tls_broker_with_its_trust_file_and_identity() {
    let schema = client_schema();
    schema
        .validate(&[
            config_entry("addr", "mqtts://broker.example.com:8883"),
            config_entry("tls_ca_file", "authority.pem"),
        ])
        .assured("the trust file is all an mqtts address requires");
    schema
        .validate(&[
            config_entry("addr", "mqtts://broker.example.com:8883"),
            config_entry("tls_ca_file", "authority.pem"),
            config_entry("tls_cert_file", "identity.pem"),
            config_entry("tls_key_file", "identity.key"),
        ])
        .assured("an mqtts connection presents the declared client identity");
}

#[rstest]
#[case::no_port("mqtt://broker.example.com")]
#[case::zero_port("mqtt://broker.example.com:0")]
#[case::empty_host("mqtt://:1883")]
#[case::user("mqtt://reader@broker.example.com:1883")]
#[case::credentials("mqtt://reader:secret@broker.example.com:1883")]
#[case::path("mqtt://broker.example.com:1883/devices")]
#[case::query("mqtt://broker.example.com:1883?keep_alive=30")]
#[case::empty_query("mqtt://broker.example.com:1883?")]
#[case::fragment("mqtt://broker.example.com:1883#devices")]
#[case::tcp_scheme("tcp://broker.example.com:1883")]
#[case::websocket_scheme("ws://broker.example.com:8080")]
#[case::not_a_url("broker.example.com:1883")]
fn client_schema_refuses_an_address_a_connection_would_not_honor(#[case] address: &str) {
    for validate in [
        ClientConfigSchema::validate,
        ClientConfigSchema::validate_rendered,
    ] {
        let refusal = validate(client_schema(), &[config_entry("addr", address)])
            .expect_err("a connection reads only the scheme, host and port of its address");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::InvalidValue {
                key: "addr".to_string(),
                expected: "URL naming only a host and a port",
            },
            "{address}"
        );
    }
}

#[test]
fn client_schema_defers_a_templated_address_to_its_rendered_value() {
    let schema = client_schema();
    schema
        .validate(&[config_entry("addr", "{{broker_address}}")])
        .assured("the declared stage keeps an unrendered template");
    let refusal = schema
        .validate_rendered(&[config_entry("addr", "mqtt://broker.example.com:1883?qos=1")])
        .expect_err("the rendered address is held to the declared form");
    assert!(
        matches!(refusal.current_context(), ClientConfigSchemaError::InvalidValue { key, .. } if key == "addr")
    );
}

#[test]
fn client_schema_requires_the_trust_file_exactly_with_mqtts() {
    let schema = client_schema();

    let missing = schema
        .validate(&[config_entry("addr", "mqtts://broker.example.com:8883")])
        .expect_err("an mqtts connection has no trust roots without its declared file");
    assert_eq!(
        missing.current_context(),
        &ClientConfigSchemaError::RequiresKey {
            key: "addr",
            required: "tls_ca_file",
        }
    );

    let plaintext = schema
        .validate(&[
            config_entry("addr", "mqtt://broker.example.com:1883"),
            config_entry("tls_ca_file", "authority.pem"),
        ])
        .expect_err("a plaintext connection reads no trust file");
    assert_eq!(
        plaintext.current_context(),
        &ClientConfigSchemaError::RequiresScheme {
            key: "tls_ca_file",
            address: "addr",
            scheme: "mqtts",
        }
    );
}

#[test]
fn client_schema_permits_a_client_identity_only_with_mqtts() {
    let schema = client_schema();

    let plaintext = schema
        .validate(&[
            config_entry("addr", "mqtt://broker.example.com:1883"),
            config_entry("tls_cert_file", "identity.pem"),
            config_entry("tls_key_file", "identity.key"),
        ])
        .expect_err("a plaintext connection presents no client certificate");
    assert_eq!(
        plaintext.current_context(),
        &ClientConfigSchemaError::RequiresScheme {
            key: "tls_cert_file",
            address: "addr",
            scheme: "mqtts",
        }
    );

    for (present, required) in [
        ("tls_cert_file", "tls_key_file"),
        ("tls_key_file", "tls_cert_file"),
    ] {
        let refusal = schema
            .validate(&[
                config_entry("addr", "mqtts://broker.example.com:8883"),
                config_entry("tls_ca_file", "authority.pem"),
                config_entry(present, "identity.pem"),
            ])
            .expect_err("one half of a TLS client identity cannot authenticate an MQTT client");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::RequiresKey {
                key: present,
                required,
            }
        );
    }
}

#[test]
fn client_schema_refuses_a_missing_address_and_an_undeclared_key() {
    let schema = client_schema();

    let missing = schema
        .validate(&[config_entry("client_id", "nervix")])
        .expect_err("an MQTT client cannot connect without its broker address");
    assert_eq!(
        missing.current_context(),
        &ClientConfigSchemaError::MissingRequired { key: "addr" }
    );

    for undeclared in ["keep_alive", "username", "password", "max_packet_size"] {
        let refusal = schema
            .validate(&[
                config_entry("addr", "mqtt://broker.example.com:1883"),
                config_entry(undeclared, "30"),
            ])
            .expect_err("every MQTT client key is Nervix-owned, so an undeclared key is a mistake");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::UnknownKey {
                key: undeclared.to_string(),
            }
        );
    }
}

#[rstest]
#[case::mixed_case("Devices/+/Orders")]
#[case::tail_wildcard("telemetry/#")]
#[case::every_topic("#")]
#[case::one_level("+")]
#[case::empty_levels("/")]
#[case::empty_middle_level("devices//orders")]
#[case::system_topics("$SYS/broker/#")]
#[case::share_inside("devices/$share/orders")]
#[case::share_prefix_of_level("$shared/devices")]
#[case::share_in_other_case("$SHARE/readers/devices")]
#[case::spaces("Sensor Readings/+")]
#[case::utf8("telemetry/Événements/東京")]
fn topic_filters_keep_their_spelling_and_whole_level_wildcards(#[case] raw: &str) {
    let filter =
        MqttTopicFilter::try_from(raw).assured("each fixture places its wildcards as whole levels");
    assert_eq!(filter.as_str(), raw);
    assert_eq!(filter.to_string(), raw);
}

#[rstest]
#[case::empty("", ExternalNameError::Empty)]
#[case::plus_inside_level("devices/se+nsor", ExternalNameError::WildcardInsideLevel { wildcard: '+' })]
#[case::hash_inside_level("devices/orders#", ExternalNameError::WildcardInsideLevel { wildcard: '#' })]
#[case::hash_before_last("devices/#/orders", ExternalNameError::WildcardBeforeLastLevel { wildcard: '#' })]
#[case::nul("devices/\0", ExternalNameError::InvalidChar { ch: '\0' })]
#[case::shared_subscription("$share/readers/devices", ExternalNameError::ReservedFirstLevel { level: "$share" })]
#[case::share_level_alone("$share", ExternalNameError::ReservedFirstLevel { level: "$share" })]
#[case::share_without_group("$share/", ExternalNameError::ReservedFirstLevel { level: "$share" })]
fn topic_filters_refuse_what_the_filter_rule_excludes(
    #[case] raw: &str,
    #[case] expected: ExternalNameError,
) {
    assert_eq!(MqttTopicFilter::try_from(raw), Err(expected));
}

#[test]
fn publication_topics_hold_no_subscription_wildcard() {
    for raw in [
        "Devices/EU/Orders",
        "sensors/Raw Topic",
        "devices//orders",
        "/",
        "$SYS/broker/load",
        "telemetry/Événements",
    ] {
        let topic = MqttTopicName::try_from(raw)
            .assured("each fixture is a literal topic without wildcards or NUL");
        assert_eq!(topic.as_str(), raw);
    }
    for (raw, ch) in [("devices/+", '+'), ("devices/#", '#'), ("devices/\0", '\0')] {
        assert_eq!(
            MqttTopicName::try_from(raw),
            Err(ExternalNameError::InvalidChar { ch })
        );
    }
    assert_eq!(MqttTopicName::try_from(""), Err(ExternalNameError::Empty));
}

#[test]
fn topics_are_bounded_by_mqtts_string_length_in_both_directions() {
    let at_bound = format!("{}/{}", "é".repeat(16_384), "x".repeat(32_766));
    assert_eq!(at_bound.len(), 65_535);
    let filter = MqttTopicFilter::try_from(at_bound.as_str())
        .assured("two levels of 32,768 and 32,766 bytes fill MQTT's 65,535-byte string");
    assert_eq!(filter.as_str(), at_bound);
    let topic = MqttTopicName::try_from(at_bound.as_str())
        .assured("the same literal topic fits the publication bound");
    assert_eq!(topic.as_str(), at_bound);

    let above_bound = format!("{at_bound}x");
    let too_long = ExternalNameError::TooLong {
        max: 65_535,
        actual: 65_536,
    };
    assert_eq!(
        MqttTopicFilter::try_from(above_bound.as_str()),
        Err(too_long.clone())
    );
    assert_eq!(MqttTopicName::try_from(above_bound.as_str()), Err(too_long));
}

#[test]
fn both_topic_literals_declare_their_examples_and_labels() {
    assert_eq!(MqttTopicFilter::DECLARATION.label, "mqtt_topic_filter");
    assert_eq!(MqttTopicName::DECLARATION.label, "mqtt_topic");
    MqttTopicFilter::try_from(MqttTopicFilter::DECLARATION.example)
        .assured("the declared filter example follows the filter rule");
    MqttTopicName::try_from(MqttTopicName::DECLARATION.example)
        .assured("the declared topic example follows the topic rule");
}
