//! Every external name keeps its text exactly through every representation, and its parser and
//! decoders accept exactly the text its entity's rule allows.
//!
//! An external name is what an external system calls the entity a source reads, case and
//! punctuation included, so a valid name is the text as written: nothing is normalized. The rule
//! each kind follows is checked at its boundaries below, and on arbitrary text the decoders agree
//! with the parser.

use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{
    ClickHouseTableName, ExternalNameError, IcebergNamespace, IcebergTableName,
    KafkaConsumerGroupName, KafkaTopicName, MongoDbCollectionName, MqttTopicFilter, MqttTopicName,
    MySqlTableName, NatsConsumerName, NatsPublishSubjectName, NatsQueueGroupName, NatsStreamName,
    NatsSubjectName, PostgresTableName, PulsarSubscriptionName, PulsarTopicName, RabbitMqQueueName,
    RedisChannelName, SqsQueueName,
};

/// Pieces arbitrary text is assembled from: what the rules admit, the separators and wildcards
/// they place, and what some refuse, such as whitespace, control characters and NUL.
const TEXT_PIECES: [&str; 24] = [
    "a",
    "Z",
    "0",
    "-",
    "_",
    ".",
    "/",
    "*",
    ">",
    "+",
    "#",
    ":",
    "//",
    "://",
    " ",
    "\t",
    "\0",
    "\n",
    "'",
    "é",
    "中文",
    ".fifo",
    "persistent",
    "non-persistent",
];

/// Arbitrary text, from empty to past the longest bound a short name has.
fn arbitrary_text(entropy: &mut Entropy<'_>) -> String {
    let pieces = entropy.boundary_biased(0..=300);
    let mut text = String::new();
    for _ in 0..pieces {
        text.push_str(entropy.pick(TEXT_PIECES));
    }
    text
}

/// Checks that the valid external name `$name` of type `$Type` keeps its text through every
/// representation.
macro_rules! assert_external_name_round_trips {
    ($Type:ident, $name:expr) => {{
        let name: $Type = $name;
        let text = name.as_str().to_owned();
        assert_eq!($Type::parse(&text).as_ref().ok(), Some(&name));
        assert_eq!(name.to_string(), text);
        assert_eq!(name.as_ref(), text);
        assert_eq!(text.parse::<$Type>().as_ref(), Ok(&name));
        assert_eq!($Type::try_from(text.as_str()).as_ref(), Ok(&name));
        assert_eq!($Type::try_from(text.clone()).as_ref(), Ok(&name));

        let json = serde_json::to_string(&name).expect("an external name has a JSON string form");
        assert_eq!(
            json,
            serde_json::to_string(&text).expect("a string has a JSON form")
        );
        let decoded: $Type = serde_json::from_str(&json).expect("a valid name decodes");
        assert_eq!(decoded, name);

        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&name).expect("an external name archives");
        let plain = rkyv::to_bytes::<rkyv::rancor::Error>(&text).expect("a string archives");
        assert_eq!(
            archived.as_slice(),
            plain.as_slice(),
            "an external name archives as its text"
        );
        let restored = rkyv::from_bytes::<$Type, rkyv::rancor::Error>(&archived)
            .expect("a valid archived name reads back");
        assert_eq!(restored, name);
    }};
}

/// Checks that the parser and both decoders of `$Type` agree on arbitrary `$text`: each accepts it
/// exactly when the rule does, and keeps it exactly as written.
macro_rules! assert_external_text_is_validated {
    ($Type:ident, $text:expr) => {{
        let text: &str = $text;
        let parsed = $Type::parse(text);
        let ruled = $Type::try_from(text);
        assert_eq!(
            parsed.as_ref().ok(),
            ruled.as_ref().ok(),
            "{} parse and conversion of {text:?}",
            stringify!($Type)
        );
        if let (Err(report), Err(rule)) = (&parsed, &ruled) {
            assert_eq!(report.current_context(), rule);
        }
        if let Ok(name) = &parsed {
            assert_eq!(name.as_str(), text);
        }
        let json = serde_json::to_string(text).expect("a string has a JSON form");
        let decoded = serde_json::from_str::<$Type>(&json);
        assert_eq!(
            decoded.ok(),
            parsed.as_ref().ok().cloned(),
            "{} JSON decoding of {text:?}",
            stringify!($Type)
        );
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&text.to_owned()).expect("a string archives");
        let restored = rkyv::from_bytes::<$Type, rkyv::rancor::Error>(&archived);
        assert_eq!(
            restored.ok(),
            parsed.ok(),
            "{} archive decoding of {text:?}",
            stringify!($Type)
        );
    }};
}

/// Expands `$check` once for every external name type, on `$text`.
macro_rules! for_every_external_name_type {
    ($assertion:ident, $text:expr) => {
        $assertion!(KafkaTopicName, $text);
        $assertion!(KafkaConsumerGroupName, $text);
        $assertion!(PulsarTopicName, $text);
        $assertion!(PulsarSubscriptionName, $text);
        $assertion!(RabbitMqQueueName, $text);
        $assertion!(SqsQueueName, $text);
        $assertion!(RedisChannelName, $text);
        $assertion!(NatsSubjectName, $text);
        $assertion!(NatsQueueGroupName, $text);
        $assertion!(NatsStreamName, $text);
        $assertion!(NatsConsumerName, $text);
        $assertion!(MqttTopicFilter, $text);
        $assertion!(ClickHouseTableName, $text);
        $assertion!(PostgresTableName, $text);
        $assertion!(MySqlTableName, $text);
        $assertion!(MongoDbCollectionName, $text);
        $assertion!(IcebergTableName, $text);
        $assertion!(IcebergNamespace, $text);
        $assertion!(MqttTopicName, $text);
        $assertion!(NatsPublishSubjectName, $text);
    };
}

#[test]
fn bolero_external_names_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            assert_external_name_round_trips!(
                ClickHouseTableName,
                arbitrary.clickhouse_table_name()
            );
            assert_external_name_round_trips!(PostgresTableName, arbitrary.postgres_table_name());
            assert_external_name_round_trips!(MySqlTableName, arbitrary.mysql_table_name());
            assert_external_name_round_trips!(
                MongoDbCollectionName,
                arbitrary.mongodb_collection_name()
            );
            assert_external_name_round_trips!(IcebergTableName, arbitrary.emitter_table_name());
            assert_external_name_round_trips!(IcebergNamespace, arbitrary.iceberg_namespace());
            assert_external_name_round_trips!(
                MqttTopicName,
                arbitrary.mqtt_publication_topic_name()
            );
            assert_external_name_round_trips!(
                NatsPublishSubjectName,
                arbitrary.nats_publication_subject_name()
            );
            assert_external_name_round_trips!(KafkaTopicName, arbitrary.kafka_topic_name());
            assert_external_name_round_trips!(
                KafkaConsumerGroupName,
                arbitrary.kafka_consumer_group_name()
            );
            assert_external_name_round_trips!(PulsarTopicName, arbitrary.pulsar_topic_name());
            assert_external_name_round_trips!(
                PulsarSubscriptionName,
                arbitrary.pulsar_subscription_name()
            );
            assert_external_name_round_trips!(RabbitMqQueueName, arbitrary.rabbitmq_queue_name());
            assert_external_name_round_trips!(SqsQueueName, arbitrary.sqs_queue_name());
            assert_external_name_round_trips!(RedisChannelName, arbitrary.redis_channel_name());
            assert_external_name_round_trips!(NatsSubjectName, arbitrary.nats_subject_name());
            assert_external_name_round_trips!(
                NatsQueueGroupName,
                arbitrary.nats_queue_group_name()
            );
            assert_external_name_round_trips!(NatsStreamName, arbitrary.nats_stream_name());
            assert_external_name_round_trips!(NatsConsumerName, arbitrary.nats_consumer_name());
            assert_external_name_round_trips!(MqttTopicFilter, arbitrary.mqtt_topic_filter());
        });
}

#[test]
fn bolero_external_name_text_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let text = arbitrary_text(arbitrary.entropy());
            for_every_external_name_type!(assert_external_text_is_validated, &text);
        });
}

/// The reason `$Type` refuses `$text`, which it must refuse.
macro_rules! refusal {
    ($Type:ident, $text:expr) => {{
        let text: &str = $text;
        $Type::try_from(text)
            .err()
            .unwrap_or_else(|| panic!("{} accepted {text:?}", stringify!($Type)))
    }};
}

#[test]
fn a_kafka_topic_follows_the_broker_rule() {
    assert!(KafkaTopicName::parse("Orders.v-1_eu").is_ok());
    assert!(KafkaTopicName::parse(&"a".repeat(249)).is_ok());
    assert_eq!(
        refusal!(KafkaTopicName, &"a".repeat(250)),
        ExternalNameError::TooLong {
            max: 249,
            actual: 250
        }
    );
    assert_eq!(refusal!(KafkaTopicName, ""), ExternalNameError::Empty);
    assert_eq!(
        refusal!(KafkaTopicName, "."),
        ExternalNameError::Reserved { name: "." }
    );
    assert_eq!(
        refusal!(KafkaTopicName, ".."),
        ExternalNameError::Reserved { name: ".." }
    );
    assert!(KafkaTopicName::parse("...").is_ok());
    assert_eq!(
        refusal!(KafkaTopicName, "orders/eu"),
        ExternalNameError::InvalidChar { ch: '/' }
    );
    assert_eq!(
        refusal!(KafkaConsumerGroupName, "readers\n"),
        ExternalNameError::InvalidChar { ch: '\n' }
    );
    assert!(KafkaConsumerGroupName::parse("Readers of orders/eu").is_ok());
}

#[test]
fn a_pulsar_topic_is_local_or_fully_qualified() {
    assert!(PulsarTopicName::parse("Orders").is_ok());
    assert!(PulsarTopicName::parse("persistent://public/default/Orders").is_ok());
    assert!(PulsarTopicName::parse("non-persistent://tenant/ns/orders").is_ok());
    assert_eq!(
        refusal!(PulsarTopicName, "public/default/orders"),
        ExternalNameError::LevelsWithoutDomain {
            separator: '/',
            known: &["persistent", "non-persistent"],
        }
    );
    assert_eq!(
        refusal!(PulsarTopicName, "persistent://public/orders"),
        ExternalNameError::QualifiedLevels {
            levels: 2,
            expected: &["tenant", "namespace", "topic"],
        }
    );
    assert_eq!(
        refusal!(PulsarTopicName, "persistent://public//orders"),
        ExternalNameError::EmptyLevel { separator: '/' }
    );
    assert_eq!(
        refusal!(PulsarTopicName, "temporary://public/default/orders"),
        ExternalNameError::UnknownDomain {
            domain: "temporary".to_owned(),
            known: &["persistent", "non-persistent"],
        }
    );
    assert_eq!(
        refusal!(PulsarTopicName, "my orders"),
        ExternalNameError::InvalidChar { ch: ' ' }
    );
    assert!(PulsarSubscriptionName::parse("Readers (eu)").is_ok());
}

#[test]
fn an_sqs_queue_follows_the_queue_rule() {
    assert!(SqsQueueName::parse("Orders-eu_1").is_ok());
    assert!(SqsQueueName::parse("Orders.fifo").is_ok());
    assert!(SqsQueueName::parse(&format!("{}.fifo", "a".repeat(75))).is_ok());
    assert_eq!(
        refusal!(SqsQueueName, &format!("{}.fifo", "a".repeat(76))),
        ExternalNameError::TooLong {
            max: 80,
            actual: 81
        }
    );
    assert_eq!(
        refusal!(SqsQueueName, ".fifo"),
        ExternalNameError::EmptyBeforeSuffix { suffix: ".fifo" }
    );
    assert_eq!(
        refusal!(SqsQueueName, "orders.eu"),
        ExternalNameError::InvalidChar { ch: '.' }
    );
}

#[test]
fn a_nats_subject_places_its_wildcards_as_whole_tokens() {
    assert!(NatsSubjectName::parse("orders.*.created").is_ok());
    assert!(NatsSubjectName::parse("orders.>").is_ok());
    assert!(NatsSubjectName::parse(">").is_ok());
    assert_eq!(
        refusal!(NatsSubjectName, "orders.>.created"),
        ExternalNameError::WildcardBeforeLastLevel { wildcard: '>' }
    );
    assert_eq!(
        refusal!(NatsSubjectName, "orders.cre*ted"),
        ExternalNameError::WildcardInsideLevel { wildcard: '*' }
    );
    assert_eq!(
        refusal!(NatsSubjectName, "orders..created"),
        ExternalNameError::EmptyLevel { separator: '.' }
    );
    assert_eq!(
        refusal!(NatsSubjectName, "orders created"),
        ExternalNameError::InvalidChar { ch: ' ' }
    );
    assert_eq!(
        refusal!(NatsQueueGroupName, "order workers"),
        ExternalNameError::InvalidChar { ch: ' ' }
    );
}

#[test]
fn a_jetstream_name_holds_no_subject_separator_wildcard_or_path_separator() {
    assert!(NatsStreamName::parse("Orders-EU").is_ok());
    assert!(NatsConsumerName::parse("nervix_readers").is_ok());
    for (text, ch) in [
        ("orders.eu", '.'),
        ("orders*", '*'),
        ("orders>", '>'),
        ("orders/eu", '/'),
        ("orders\\eu", '\\'),
    ] {
        assert_eq!(
            refusal!(NatsStreamName, text),
            ExternalNameError::InvalidChar { ch }
        );
        assert_eq!(
            refusal!(NatsConsumerName, text),
            ExternalNameError::InvalidChar { ch }
        );
    }
}

#[test]
fn an_mqtt_topic_filter_places_its_wildcards_as_whole_levels() {
    assert!(MqttTopicFilter::parse("devices/+/Orders").is_ok());
    assert!(MqttTopicFilter::parse("devices/#").is_ok());
    assert!(MqttTopicFilter::parse("/").is_ok());
    assert!(MqttTopicFilter::parse("orders created").is_ok());
    assert_eq!(
        refusal!(MqttTopicFilter, "devices/#/orders"),
        ExternalNameError::WildcardBeforeLastLevel { wildcard: '#' }
    );
    assert_eq!(
        refusal!(MqttTopicFilter, "devices/a+"),
        ExternalNameError::WildcardInsideLevel { wildcard: '+' }
    );
    assert_eq!(
        refusal!(MqttTopicFilter, "devices\0"),
        ExternalNameError::InvalidChar { ch: '\0' }
    );
    assert_eq!(refusal!(MqttTopicFilter, ""), ExternalNameError::Empty);
}

#[test]
fn a_queue_or_channel_admits_any_text_within_its_bound() {
    assert!(RabbitMqQueueName::parse("orders\twith tab").is_ok());
    assert!(RabbitMqQueueName::parse(&"q".repeat(255)).is_ok());
    assert_eq!(
        refusal!(RabbitMqQueueName, &"q".repeat(256)),
        ExternalNameError::TooLong {
            max: 255,
            actual: 256
        }
    );
    assert!(RedisChannelName::parse("orders:*").is_ok());
    assert_eq!(
        refusal!(RedisChannelName, &"c".repeat(1025)),
        ExternalNameError::TooLong {
            max: 1024,
            actual: 1025
        }
    );
}

#[test]
fn mysql_table_paths_keep_one_or_two_nonempty_parts() {
    for text in ["Orders", "Shop.Orders", "shop-1.Orders_2"] {
        let name = MySqlTableName::parse(text).expect("a current table path");
        assert_eq!(
            name.parts().collect::<Vec<_>>(),
            text.split('.').collect::<Vec<_>>()
        );
        assert_external_name_round_trips!(MySqlTableName, name);
    }
    let boundary = format!("{}.{}", "D".repeat(63), "T".repeat(64));
    assert_external_name_round_trips!(
        MySqlTableName,
        MySqlTableName::parse(&boundary).expect("the complete qualified path reaches 128 bytes")
    );
    for text in [".Orders", "Shop.", "Shop..Orders"] {
        assert_eq!(
            refusal!(MySqlTableName, text),
            ExternalNameError::EmptyLevel { separator: '.' }
        );
    }
    assert_eq!(
        refusal!(MySqlTableName, "region.Shop.Orders"),
        ExternalNameError::TooManyLevels { max: 2, actual: 3 }
    );
}
