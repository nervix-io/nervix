//! Tests of the Kafka vocabulary's executable topic contract.
//!
//! Layer: test harness.
//! - **Owns.** Kafka declaration, client configuration, topic-name and consumer-group-name
//!   contract checks through the public vocabulary.
//! - **Depends on.** Kafka Models, entity kinds and shared declaration types.
//! - **Must not know.** librdkafka, parsing, planning or how a topic is consumed or produced to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigNamespace, ClientConfigOwner, ClientConfigPresence,
    ClientConfigSchemaError, ClientConfigValue, ClientPooling, CreateClientKafka,
    EmitterEntityKind, EmitterPublishingModeKind, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityKind,
};

/// The keys Nervix sets on every Kafka consumer or producer it runs.
const NERVIX_OWNED: [&str; 5] = [
    "allow.auto.create.topics",
    "delivery.report.only.error",
    "enable.auto.commit",
    "enable.partition.eof",
    "group.id",
];

#[test]
fn topic_declarations_match_the_replayable_log_and_its_producer() {
    let ingest = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic).capabilities();

    assert_eq!(
        ingest.delivery,
        &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL
        ]
    );
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::AckRequest),
        None
    );
    assert_eq!(ingest.position_class, PositionClass::Replayable);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::Answered
    );
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(ingest.quiesce, &[DeclaredQuiesce::SUSPEND]);
    for undeclared in [
        IngestQuiesceKind::Buffer,
        IngestQuiesceKind::Drop,
        IngestQuiesceKind::Reject,
        IngestQuiesceKind::EndpointBuffer,
    ] {
        assert_eq!(ingest.declared_quiesce(undeclared), None);
    }
    assert_eq!(
        emit.publishing_modes,
        &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::BrokerAck
        ]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert!(!emit.requires_batch_transformation);
}

#[test]
fn both_topic_directions_share_the_topic_rule_and_the_client() {
    let ingest = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic).capabilities();
    let topic = Some(LocatorDeclaration {
        keywords: "TOPIC",
        name: LocatorName::External(KafkaTopicName::DECLARATION),
    });

    assert_eq!(ingest.locator, topic);
    assert_eq!(emit.locator, topic);
    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Kafka)
    );
    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Kafka)
    );
    assert_eq!(
        CreateClientKafka::<u64>::DECLARATION.pooling,
        ClientPooling::Unpooled
    );
    assert!(CreateClientKafka::<u64>::DECLARATION.options.is_empty());
}

#[test]
fn records_carry_ordered_repeated_headers_and_their_log_position() {
    let ingest = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic).capabilities();
    let headers = Some(HeaderModel::exact(HeaderLayout::OrderedRepeats));

    assert_eq!(ingest.headers, headers);
    assert_eq!(emit.headers, headers);
    assert_eq!(
        ingest.metadata.fields(),
        [
            IngestorMetadataField::new("topic", IngestorMetadataType::String),
            IngestorMetadataField::new("partition", IngestorMetadataType::I32),
            IngestorMetadataField::new("offset", IngestorMetadataType::I64),
        ]
    );
    assert!(emit.options.is_empty());
    assert!(emit.envelope.is_empty());
    assert!(emit.properties.is_empty());
}

#[test]
fn offsets_belong_to_the_domain_or_to_a_named_consumer_group() {
    let ingest = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic).capabilities();
    let [offsets, instances] = ingest.options else {
        panic!("a Kafka topic ingestor declares its offset ownership and its instances");
    };

    assert_eq!(*instances, OptionDeclaration::INSTANCES);
    assert_eq!(offsets.keywords, "OFFSET BY");
    assert_eq!(offsets.presence, OptionPresence::Required);
    let OptionForm::Choice([domain, group]) = offsets.form else {
        panic!("OFFSET BY chooses between the domain and a consumer group");
    };
    assert_eq!(domain.name, "DOMAIN");
    assert_eq!(domain.value, None);
    assert_eq!(group.name, "CONSUMER GROUP");
    assert_eq!(
        group.value,
        Some(OptionValue::Name(KafkaConsumerGroupName::DECLARATION))
    );
}

#[test]
fn batching_is_optional_and_bounded_by_what_the_producer_learns() {
    let emit = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic).capabilities();

    assert_eq!(emit.batch, EmitterBatchLegality::Optional { maximum: None });
}

#[test]
fn no_client_deadline_key_bounds_a_kafka_wait() {
    let ingest = IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic).capabilities();

    assert!(ingest.bounds.deadline_keys().is_empty());
    assert!(emit.bounds.deadline_keys().is_empty());
    let confirmed_delivery = emit
        .bounds
        .waits
        .iter()
        .find(|wait| wait.wait == "delivery under ACK")
        .assured("the emitter declares its confirmed delivery wait");
    assert_eq!(confirmed_delivery.bound, WaitBound::Clause("ACK TIMEOUT"));
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn client_schema_requires_the_broker_list_and_passes_every_property_to_the_driver() {
    let config = CreateClientKafka::<u64>::DECLARATION.config;
    let bootstrap = config
        .key("bootstrap.servers")
        .assured("the Kafka client declares its broker list");

    assert_eq!(bootstrap.presence, ClientConfigPresence::Required);
    assert_eq!(bootstrap.value, ClientConfigValue::String);
    for key in config.keys() {
        assert_eq!(
            key.owner,
            ClientConfigOwner::Driver,
            "librdkafka interprets '{}'",
            key.name
        );
    }
    for secret in [
        "sasl.oauthbearer.config",
        "sasl.password",
        "sasl.username",
        "ssl.key.password",
        "ssl.key.pem",
    ] {
        let key = config
            .key(secret)
            .assured("the declared key list names every Kafka credential");
        assert!(key.confidential, "{secret} is redacted by inspection");
    }
    assert_eq!(config.namespaces(), &[ClientConfigNamespace::All]);
    assert_eq!(config.reserved(), &NERVIX_OWNED);
    assert_eq!(
        config.ownership_label(),
        "declared keys and driver-delegated property namespaces"
    );
}

#[test]
fn client_schema_refuses_a_client_without_a_broker_list() {
    let refusal = CreateClientKafka::<u64>::DECLARATION
        .config
        .validate(&[config_entry("client.id", "nervix")])
        .expect_err("a Kafka client reaches no broker without bootstrap.servers");

    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::MissingRequired {
            key: "bootstrap.servers"
        }
    );
}

#[test]
fn client_schema_refuses_every_key_nervix_sets() {
    for key in NERVIX_OWNED {
        let refusal = CreateClientKafka::<u64>::DECLARATION
            .config
            .validate(&[
                config_entry("bootstrap.servers", "127.0.0.1:9092"),
                config_entry(key, "value"),
            ])
            .expect_err("Nervix sets this key on every Kafka consumer or producer it runs");
        assert!(
            matches!(refusal.current_context(), ClientConfigSchemaError::Reserved { key: refused } if refused == key)
        );
    }
}

#[test]
fn client_schema_accepts_the_broker_list_with_any_driver_property() {
    CreateClientKafka::<u64>::DECLARATION
        .config
        .validate(&[
            config_entry("bootstrap.servers", "broker-1:9092,broker-2:9092"),
            config_entry("security.protocol", "sasl_ssl"),
            config_entry("sasl.password", "declared-secret"),
            config_entry("topic.metadata.refresh.interval.ms", "1000"),
        ])
        .assured("librdkafka owns every property beside the broker list and checks it offline");
}

#[rstest]
#[case("orders")]
#[case("Orders.EU_v2-1")]
#[case("..orders")]
#[case("a")]
fn topic_names_preserve_every_literal_character(#[case] raw: &str) {
    let topic = KafkaTopicName::try_from(raw).assured("each fixture is a valid Kafka topic");

    assert_eq!(topic.as_str(), raw);
    assert_eq!(topic.to_string(), raw);
}

#[test]
fn a_topic_fills_at_most_249_bytes() {
    let at_bound = "t".repeat(249);
    let topic =
        KafkaTopicName::try_from(at_bound.as_str()).assured("249 bytes fit the Kafka topic bound");
    assert_eq!(topic.as_str(), at_bound);

    let above_bound = "t".repeat(250);
    assert_eq!(
        KafkaTopicName::try_from(above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 249,
            actual: 250,
        })
    );
}

#[rstest]
#[case("", ExternalNameError::Empty)]
#[case(".", ExternalNameError::Reserved { name: "." })]
#[case("..", ExternalNameError::Reserved { name: ".." })]
#[case("orders/eu", ExternalNameError::InvalidChar { ch: '/' })]
#[case("Orders EU", ExternalNameError::InvalidChar { ch: ' ' })]
#[case("orders:eu", ExternalNameError::InvalidChar { ch: ':' })]
#[case("commandes-été", ExternalNameError::InvalidChar { ch: 'é' })]
fn topic_names_outside_the_kafka_rule_are_refused(
    #[case] raw: &str,
    #[case] refusal: ExternalNameError,
) {
    assert_eq!(KafkaTopicName::try_from(raw), Err(refusal));
}

#[rstest]
#[case("readers")]
#[case("Readers EU")]
#[case("nervix/orders.v2")]
#[case("lecteurs-été")]
fn consumer_group_names_preserve_every_literal_character(#[case] raw: &str) {
    let group =
        KafkaConsumerGroupName::try_from(raw).assured("each fixture is a valid consumer group");

    assert_eq!(group.as_str(), raw);
}

#[test]
fn a_consumer_group_fills_at_most_255_bytes() {
    let at_bound = "g".repeat(255);
    let group = KafkaConsumerGroupName::try_from(at_bound.as_str())
        .assured("255 bytes fit the consumer group bound");
    assert_eq!(group.as_str(), at_bound);

    let multibyte_above_bound = "é".repeat(128);
    assert_eq!(
        KafkaConsumerGroupName::try_from(multibyte_above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 255,
            actual: 256,
        })
    );
}

#[rstest]
#[case("", ExternalNameError::Empty)]
#[case("readers\tEU", ExternalNameError::InvalidChar { ch: '\t' })]
#[case("readers\u{0}", ExternalNameError::InvalidChar { ch: '\u{0}' })]
fn consumer_group_names_outside_the_rule_are_refused(
    #[case] raw: &str,
    #[case] refusal: ExternalNameError,
) {
    assert_eq!(KafkaConsumerGroupName::try_from(raw), Err(refusal));
}
