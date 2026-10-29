//! Tests of the NATS vocabulary's executable Core subject and JetStream stream contracts.
//!
//! Layer: test harness.
//! - **Owns.** NATS declaration, subject, queue-group, stream, consumer and client-configuration
//!   contract checks through the public vocabulary.
//! - **Depends on.** NATS Models, entity kinds and shared declaration types.
//! - **Must not know.** The NATS driver, parsing, planning or runtime subscriptions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigFile, ClientConfigOwner, ClientConfigPresence,
    ClientConfigSchemaError, ClientConfigValue, ClientPooling, EmitterEntityKind,
    EmitterPublishingModeKind, IngestDeliveryKind, IngestQuiesceKind, IngestorEntityKind,
};

#[test]
fn subject_ingestion_declares_an_unacknowledged_queue_subscription() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Subject).capabilities();

    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Nats)
    );
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(ingest.delivery, &[DeclaredDelivery::NO_ACK]);
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::AckSequential),
        None
    );
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::AckParallel),
        None
    );
    assert_eq!(ingest.position_class, PositionClass::None);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::None
    );
    assert_eq!(
        ingest.quiesce,
        &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP]
    );
    assert_eq!(ingest.declared_quiesce(IngestQuiesceKind::Suspend), None);
}

#[test]
fn subject_ingestion_requires_its_queue_group_and_instance_count() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Subject).capabilities();

    assert_eq!(
        ingest.locator,
        Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::External(NatsSubjectName::DECLARATION),
        })
    );
    let keywords: Vec<&str> = ingest
        .options
        .iter()
        .map(|option| option.keywords)
        .collect();
    assert_eq!(keywords, vec!["QUEUE GROUP", "INSTANCES"]);
    let queue_group = ingest
        .option("QUEUE GROUP")
        .assured("the subject ingestor declares its queue group option");
    assert!(queue_group.is_required());
    assert_eq!(
        queue_group.form,
        OptionForm::Value {
            value: OptionValue::Name(NatsQueueGroupName::DECLARATION),
            example: "QUEUE GROUP 'workers'",
        }
    );
    assert_eq!(
        ingest.option("INSTANCES"),
        Some(&OptionDeclaration::INSTANCES.required())
    );
}

#[test]
fn subject_emission_declares_core_and_jetstream_publication() {
    let emit = EmitterEntityKind::Nats(NatsEmitterEntity::Subject).capabilities();

    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Nats)
    );
    assert_eq!(
        emit.publishing_modes,
        &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::NatsJetStream,
        ]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert_eq!(emit.batch, EmitterBatchLegality::Optional { maximum: None });
    assert!(!emit.requires_batch_transformation);
    assert_eq!(
        emit.locator,
        Some(LocatorDeclaration {
            keywords: "SUBJECT",
            name: LocatorName::External(NatsPublishSubjectName::DECLARATION),
        })
    );
    assert!(emit.options.is_empty());
    assert!(emit.envelope.is_empty());
    assert!(emit.properties.is_empty());
}

#[test]
fn both_subject_directions_carry_headers_by_name_and_no_typed_metadata() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Subject).capabilities();
    let emit = EmitterEntityKind::Nats(NatsEmitterEntity::Subject).capabilities();
    let headers = Some(HeaderModel::exact(HeaderLayout::RepeatsByName));

    assert_eq!(ingest.headers, headers);
    assert_eq!(emit.headers, headers);
    assert!(ingest.reads_headers());
    assert_eq!(ingest.metadata, IngestorMetadataScope::Headers);
    assert!(ingest.metadata.fields().is_empty());
}

#[test]
fn subject_ingestion_declares_the_drivers_read_ahead_and_unkeyed_waits() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Subject).capabilities();

    assert_eq!(ingest.bounds.buffers.len(), 1);
    assert_eq!(
        ingest.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Driver("65,536 messages"),
            overflow: BufferOverflow::DropUncounted,
        }
    );
    let waits: Vec<WaitBound> = ingest.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        waits,
        vec![
            WaitBound::Unbounded,
            WaitBound::Driver("5 s per address"),
            WaitBound::Unbounded,
            WaitBound::Driver("a ping every 60 s; the connection ends after 2 go unanswered"),
        ]
    );
    assert!(ingest.bounds.deadline_keys().is_empty());
    assert!(ingest.bounds.driver_limits.is_empty());
}

#[test]
fn subject_emission_bounds_a_core_write_by_its_client_timeout() {
    let emit = EmitterEntityKind::Nats(NatsEmitterEntity::Subject).capabilities();

    assert_eq!(emit.bounds.buffers.len(), 1);
    assert_eq!(
        emit.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Driver("2048 commands"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    let waits: Vec<WaitBound> = emit.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        waits,
        vec![
            WaitBound::Unbounded,
            WaitBound::Driver("5 s per address"),
            WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
            WaitBound::Clause("ACK TIMEOUT"),
        ]
    );
    assert_eq!(emit.bounds.deadline_keys(), vec![DeadlineKey::Timeout]);
    assert_eq!(
        emit.bounds.driver_limits,
        &["Messages above the server's max_payload are rejected."]
    );
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn client_declaration_keeps_the_address_at_the_driver_boundary() {
    let declaration = CreateClientNats::<u64>::DECLARATION;
    assert_eq!(CreateClientNats::<u64>::CONNECTOR, ConnectorKind::Nats);
    assert_eq!(declaration.pooling, ClientPooling::Unpooled);
    assert!(declaration.options.is_empty());

    let config = declaration.config;
    let names: Vec<&str> = config.keys().iter().map(|key| key.name).collect();
    assert_eq!(
        names,
        vec![
            "addr",
            "timeout_ms",
            "tls_ca_file",
            "tls_cert_file",
            "tls_key_file"
        ]
    );
    for key in config.keys() {
        assert_eq!(key.owner, ClientConfigOwner::Nervix, "{}", key.name);
        assert!(!key.confidential, "{}", key.name);
    }
    let address = config
        .key("addr")
        .assured("the NATS client declares its server address");
    assert_eq!(address.presence, ClientConfigPresence::Required);
    assert_eq!(address.value, ClientConfigValue::DriverAddress);
    for (name, value) in [
        ("timeout_ms", ClientConfigValue::Milliseconds),
        (
            "tls_ca_file",
            ClientConfigValue::File(ClientConfigFile::CertificateAuthority),
        ),
        (
            "tls_cert_file",
            ClientConfigValue::File(ClientConfigFile::Certificate),
        ),
        (
            "tls_key_file",
            ClientConfigValue::File(ClientConfigFile::PrivateKey),
        ),
    ] {
        let key = config
            .key(name)
            .assured("the fixture lists only keys the NATS client declares");
        assert_eq!(key.presence, ClientConfigPresence::Optional, "{name}");
        assert_eq!(key.value, value, "{name}");
    }
    assert_eq!(
        config.ownership_label(),
        "Nervix; address query delegated to driver"
    );
    assert!(config.namespaces().is_empty());
    assert!(config.reserved().is_empty());
}

#[test]
fn client_schema_accepts_the_current_connection_settings() {
    let schema = CreateClientNats::<u64>::DECLARATION.config;
    schema
        .validate(&[config_entry("addr", "nats://reader:secret@127.0.0.1:4222")])
        .assured("URL userinfo stays with the driver, which authenticates with it");
    schema
        .validate(&[
            config_entry("addr", "tls://nats.example.com:4222"),
            config_entry("timeout_ms", "2500"),
            config_entry("tls_ca_file", "trust.pem"),
            config_entry("tls_cert_file", "identity.pem"),
            config_entry("tls_key_file", "identity.key"),
        ])
        .assured("every declared key admits a value of its declared type");
}

#[rstest]
#[case::zero_timeout("timeout_ms", "0")]
#[case::fractional_timeout("timeout_ms", "1.5s")]
#[case::empty_address("addr", "")]
fn client_schema_refuses_values_outside_their_declared_type(
    #[case] key: &str,
    #[case] value: &str,
) {
    let mut entries = vec![config_entry(key, value)];
    if key != "addr" {
        entries.push(config_entry("addr", "nats://127.0.0.1:4222"));
    }
    let refusal = CreateClientNats::<u64>::DECLARATION
        .config
        .validate(&entries)
        .expect_err("a Nervix-owned key's value must satisfy its declared type");
    assert!(
        matches!(refusal.current_context(), ClientConfigSchemaError::InvalidValue { key: refused, .. } if refused == key)
    );
}

#[test]
fn client_schema_refuses_a_missing_address_and_an_undeclared_key() {
    let schema = CreateClientNats::<u64>::DECLARATION.config;

    let missing = schema
        .validate(&[config_entry("timeout_ms", "100")])
        .expect_err("a NATS client cannot connect without an address");
    assert_eq!(
        missing.current_context(),
        &ClientConfigSchemaError::MissingRequired { key: "addr" }
    );

    let undeclared = schema
        .validate(&[
            config_entry("addr", "nats://127.0.0.1:4222"),
            config_entry("adrr", "nats://127.0.0.1:4222"),
        ])
        .expect_err("every NATS client key is Nervix-owned, so an undeclared key is a mistake");
    assert_eq!(
        undeclared.current_context(),
        &ClientConfigSchemaError::UnknownKey {
            key: "adrr".to_string(),
        }
    );
}

#[test]
fn client_schema_requires_both_halves_of_a_tls_identity() {
    let schema = CreateClientNats::<u64>::DECLARATION.config;
    let address = config_entry("addr", "tls://127.0.0.1:4222");
    for (present, required) in [
        ("tls_cert_file", "tls_key_file"),
        ("tls_key_file", "tls_cert_file"),
    ] {
        let refusal = schema
            .validate(&[address.clone(), config_entry(present, "identity.pem")])
            .expect_err("one half of a TLS client identity cannot authenticate a NATS client");
        assert_eq!(
            refusal.current_context(),
            &ClientConfigSchemaError::RequiresKey {
                key: present,
                required,
            }
        );
    }
}

#[rstest]
#[case::mixed_case("Orders.Created")]
#[case::single_token_wildcard("orders.*.EU")]
#[case::tail_wildcard("orders.>")]
#[case::only_wildcards("*.>")]
#[case::every_token(">")]
#[case::system_prefix("$JS.API.STREAM.INFO.Orders")]
#[case::utf8("telemetry.Événements.東京")]
fn subscription_subjects_keep_their_spelling_and_whole_token_wildcards(#[case] raw: &str) {
    let subject = NatsSubjectName::try_from(raw)
        .assured("each fixture has nonempty tokens and wildcards only as whole tokens");
    assert_eq!(subject.as_str(), raw);
    assert_eq!(subject.to_string(), raw);
}

#[rstest]
#[case::empty("", ExternalNameError::Empty)]
#[case::empty_middle_token("orders..created", ExternalNameError::EmptyLevel { separator: '.' })]
#[case::leading_separator(".orders", ExternalNameError::EmptyLevel { separator: '.' })]
#[case::trailing_separator("orders.", ExternalNameError::EmptyLevel { separator: '.' })]
#[case::star_inside_token("orders.cre*", ExternalNameError::WildcardInsideLevel { wildcard: '*' })]
#[case::tail_inside_token("orders.>x", ExternalNameError::WildcardInsideLevel { wildcard: '>' })]
#[case::tail_before_last("orders.>.created", ExternalNameError::WildcardBeforeLastLevel { wildcard: '>' })]
#[case::space("orders created", ExternalNameError::InvalidChar { ch: ' ' })]
#[case::tab("orders\tcreated", ExternalNameError::InvalidChar { ch: '\t' })]
#[case::control("orders.\u{7}", ExternalNameError::InvalidChar { ch: '\u{7}' })]
fn subscription_subjects_refuse_what_the_subject_rule_excludes(
    #[case] raw: &str,
    #[case] expected: ExternalNameError,
) {
    assert_eq!(NatsSubjectName::try_from(raw), Err(expected));
}

#[test]
fn publication_subjects_follow_the_subject_rule_without_wildcards() {
    for raw in [
        "Orders.Created",
        "$JS.API.STREAM.INFO.Orders",
        "telemetry.Événements",
    ] {
        let subject = NatsPublishSubjectName::try_from(raw)
            .assured("each fixture is a literal subject without wildcards");
        assert_eq!(subject.as_str(), raw);
    }
    assert_eq!(
        NatsPublishSubjectName::try_from("orders.*"),
        Err(ExternalNameError::InvalidChar { ch: '*' })
    );
    assert_eq!(
        NatsPublishSubjectName::try_from("orders.>"),
        Err(ExternalNameError::InvalidChar { ch: '>' })
    );
    assert_eq!(
        NatsPublishSubjectName::try_from("orders..created"),
        Err(ExternalNameError::EmptyLevel { separator: '.' })
    );
    assert_eq!(
        NatsPublishSubjectName::try_from("orders created"),
        Err(ExternalNameError::InvalidChar { ch: ' ' })
    );
}

#[test]
fn subjects_are_bounded_in_utf8_bytes_in_both_directions() {
    let at_bound = format!("{}.{}", "é".repeat(256), "x".repeat(511));
    assert_eq!(at_bound.len(), 1024);
    let subscribed = NatsSubjectName::try_from(at_bound.as_str())
        .assured("two tokens of 512 and 511 bytes fill the declared 1,024-byte bound");
    assert_eq!(subscribed.as_str(), at_bound);
    let published = NatsPublishSubjectName::try_from(at_bound.as_str())
        .assured("the same literal subject fits the publication bound");
    assert_eq!(published.as_str(), at_bound);

    let above_bound = format!("{at_bound}x");
    let too_long = ExternalNameError::TooLong {
        max: 1024,
        actual: 1025,
    };
    assert_eq!(
        NatsSubjectName::try_from(above_bound.as_str()),
        Err(too_long.clone())
    );
    assert_eq!(
        NatsPublishSubjectName::try_from(above_bound.as_str()),
        Err(too_long)
    );
}

#[rstest]
#[case::lower("workers")]
#[case::mixed_case("Readers-EU")]
#[case::dotted("billing.readers")]
#[case::utf8("lecteurs-é")]
fn queue_groups_are_joined_exactly_as_written(#[case] raw: &str) {
    let group = NatsQueueGroupName::try_from(raw)
        .assured("each fixture is a nonempty queue group without whitespace");
    assert_eq!(group.as_str(), raw);
}

#[test]
fn queue_groups_are_bounded_and_refuse_whitespace() {
    let at_bound = "q".repeat(255);
    let group = NatsQueueGroupName::try_from(at_bound.as_str())
        .assured("255 bytes fit the declared queue-group bound");
    assert_eq!(group.as_str(), at_bound);
    assert_eq!(
        NatsQueueGroupName::try_from("q".repeat(256).as_str()),
        Err(ExternalNameError::TooLong {
            max: 255,
            actual: 256,
        })
    );
    assert_eq!(
        NatsQueueGroupName::try_from(""),
        Err(ExternalNameError::Empty)
    );
    assert_eq!(
        NatsQueueGroupName::try_from("work ers"),
        Err(ExternalNameError::InvalidChar { ch: ' ' })
    );
    assert_eq!(
        NatsQueueGroupName::try_from("workers\n"),
        Err(ExternalNameError::InvalidChar { ch: '\n' })
    );
}

#[test]
fn stream_ingestion_declares_acknowledged_pulls_answered_by_the_server() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Stream).capabilities();

    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Nats)
    );
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(
        ingest.delivery,
        &[
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL
        ]
    );
    assert_eq!(ingest.declared_delivery(IngestDeliveryKind::NoAck), None);
    assert_eq!(ingest.position_class, PositionClass::BrokerRedelivered);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::Answered
    );
    assert_eq!(
        ingest.quiesce,
        &[
            DeclaredQuiesce::SUSPEND,
            DeclaredQuiesce::BUFFER,
            DeclaredQuiesce::DROP,
        ]
    );
    assert_eq!(
        ingest.headers,
        Some(HeaderModel::exact(HeaderLayout::RepeatsByName))
    );
    assert_eq!(ingest.metadata, IngestorMetadataScope::Headers);
    assert!(ingest.metadata.fields().is_empty());
}

#[test]
fn stream_ingestion_requires_its_consumer_and_names_no_instance_count() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Stream).capabilities();

    assert_eq!(
        ingest.locator,
        Some(LocatorDeclaration {
            keywords: "STREAM",
            name: LocatorName::External(NatsStreamName::DECLARATION),
        })
    );
    let keywords: Vec<&str> = ingest
        .options
        .iter()
        .map(|option| option.keywords)
        .collect();
    assert_eq!(keywords, vec!["CONSUMER"]);
    let consumer = ingest
        .option("CONSUMER")
        .assured("the stream ingestor declares its consumer option");
    assert!(consumer.is_required());
    assert_eq!(
        consumer.form,
        OptionForm::Value {
            value: OptionValue::Name(NatsConsumerName::DECLARATION),
            example: "CONSUMER 'nervix'",
        }
    );
    assert_eq!(ingest.option("INSTANCES"), None);
}

#[test]
fn stream_ingestion_bounds_what_a_pull_holds_by_its_acknowledgement_window() {
    let ingest = IngestorEntityKind::Nats(NatsIngestorEntity::Stream).capabilities();

    assert_eq!(ingest.bounds.buffers.len(), 1);
    assert_eq!(
        ingest.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    let waits: Vec<WaitBound> = ingest.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        waits,
        vec![
            WaitBound::Unbounded,
            WaitBound::Driver("5 s per address"),
            WaitBound::Driver("5 s request timeout"),
            WaitBound::Driver("10 s request timeout"),
            WaitBound::Driver("a ping every 60 s; the connection ends after 2 go unanswered"),
        ]
    );
    assert!(ingest.bounds.deadline_keys().is_empty());
    assert_eq!(ingest.bounds.driver_limits.len(), 1);
}

#[test]
fn stream_modes_take_their_acknowledgement_windows_from_the_statement() {
    let retry_policy = RetryPolicy {
        backoff: "100ms".to_string(),
        max_backoff: "1s".to_string(),
    };
    let sequential = NatsStreamIngestMode::AckSequential {
        timeout: "10s".to_string(),
        retry_policy: retry_policy.clone(),
    };
    assert_eq!(
        sequential.acknowledgement(),
        IngestAcknowledgement::Sequential {
            timeout: "10s",
            retry: &retry_policy,
        }
    );
    let max = NonZeroU64::new(4).assured("a fixed window of four is positive");
    let parallel = NatsStreamIngestMode::AckParallel {
        max,
        batch_timeout: "50ms".to_string(),
        timeout: "10s".to_string(),
        retry_policy: retry_policy.clone(),
    };
    assert_eq!(
        parallel.acknowledgement(),
        IngestAcknowledgement::Parallel {
            max,
            batch_timeout: "50ms",
            timeout: "10s",
            retry: &retry_policy,
        }
    );
}

#[rstest]
#[case::lower("orders")]
#[case::mixed_case("Orders-EU")]
#[case::underscores("orders_2026")]
#[case::utf8("Événements-東京")]
#[case::punctuation("orders:v2$")]
fn stream_and_consumer_names_are_kept_exactly_as_written(#[case] raw: &str) {
    let stream = NatsStreamName::try_from(raw)
        .assured("each fixture holds no separator, wildcard, whitespace or control character");
    assert_eq!(stream.as_str(), raw);
    let consumer =
        NatsConsumerName::try_from(raw).assured("a consumer name follows the stream name rule");
    assert_eq!(consumer.as_str(), raw);
}

#[rstest]
#[case::empty("", ExternalNameError::Empty)]
#[case::subject_separator("orders.eu", ExternalNameError::InvalidChar { ch: '.' })]
#[case::token_wildcard("orders*", ExternalNameError::InvalidChar { ch: '*' })]
#[case::tail_wildcard("orders>", ExternalNameError::InvalidChar { ch: '>' })]
#[case::path_separator("orders/eu", ExternalNameError::InvalidChar { ch: '/' })]
#[case::windows_path_separator("orders\\eu", ExternalNameError::InvalidChar { ch: '\\' })]
#[case::space("orders eu", ExternalNameError::InvalidChar { ch: ' ' })]
#[case::control("orders\u{7}", ExternalNameError::InvalidChar { ch: '\u{7}' })]
fn stream_and_consumer_names_refuse_what_jetstream_refuses(
    #[case] raw: &str,
    #[case] expected: ExternalNameError,
) {
    assert_eq!(NatsStreamName::try_from(raw), Err(expected.clone()));
    assert_eq!(NatsConsumerName::try_from(raw), Err(expected));
}

#[test]
fn stream_and_consumer_names_are_bounded_at_the_servers_name_length() {
    let at_bound = format!("{}{}", "é".repeat(127), "x");
    assert_eq!(at_bound.len(), 255);
    let stream = NatsStreamName::try_from(at_bound.as_str())
        .assured("255 bytes fit the JetStream name bound");
    assert_eq!(stream.as_str(), at_bound);
    let consumer = NatsConsumerName::try_from(at_bound.as_str())
        .assured("255 bytes fit the JetStream name bound");
    assert_eq!(consumer.as_str(), at_bound);

    let above_bound = format!("{at_bound}x");
    let too_long = ExternalNameError::TooLong {
        max: 255,
        actual: 256,
    };
    assert_eq!(
        NatsStreamName::try_from(above_bound.as_str()),
        Err(too_long.clone())
    );
    assert_eq!(
        NatsConsumerName::try_from(above_bound.as_str()),
        Err(too_long)
    );
}
