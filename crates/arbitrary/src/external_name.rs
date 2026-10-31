//! External names: what an external system calls the topics, queues, channels, subjects, consumer
//! groups and subscriptions a source reads, spelled as that system admits them.
//!
//! Each generator writes text its name's rule accepts, reaching the rule's length bound as often
//! as anywhere else and using the case, punctuation and wildcards the rule admits, so a property
//! meets the spellings a string literal has to keep exactly. A generator whose bytes ran out
//! writes the smallest name the rule admits.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClickHouseTableName, ExternalName, KafkaConsumerGroupName, KafkaTopicName,
    MONGODB_NAMESPACE_MAX_BYTES, MongoDbCollectionName, MqttTopicFilter, MqttTopicName,
    MySqlTableName, NatsPublishSubjectName, NatsQueueGroupName, NatsSubjectName, PostgresTableName,
    PulsarSubscriptionName, PulsarTopicName, RabbitMqQueueName, RedisChannelName,
    RedisChannelPattern, SqsQueueName,
};

use crate::Arbitrary;

/// The longest Kafka topic, in bytes.
const KAFKA_TOPIC_BYTES: usize = 249;
/// The longest Kafka consumer group, NATS queue group, Pulsar subscription or RabbitMQ queue, in
/// bytes.
const SHORT_NAME_BYTES: usize = 255;
/// The longest Pulsar topic, NATS subject, Redis channel or Redis channel pattern, in bytes.
const LONG_NAME_BYTES: usize = 1024;
/// The longest SQS queue name, its `.fifo` suffix included.
const SQS_QUEUE_BYTES: usize = 80;
/// The suffix of a FIFO queue's name.
const SQS_FIFO_SUFFIX: &str = ".fifo";
/// The longest MQTT topic filter, in bytes.
const MQTT_TOPIC_FILTER_BYTES: usize = 65_535;
/// The longest level or token of a filter or subject that holds several, in bytes.
const LEVEL_BYTES: usize = 32;
/// The most levels or tokens a generated filter or subject holds.
const MOST_LEVELS: usize = 6;

/// What a Kafka topic is spelled from.
const KAFKA_TOPIC_PIECES: [&str; 7] = ["a", "Z", "0", "9", ".", "_", "-"];
/// What an SQS queue name is spelled from before its suffix.
const SQS_QUEUE_PIECES: [&str; 6] = ["a", "Z", "0", "9", "-", "_"];
/// What a name with no rule but its length and the absence of control characters is spelled from:
/// letters of both cases, digits, the punctuation external systems put in names, the characters a
/// string literal has to quote, a space, and text outside ASCII.
const PRINTABLE_PIECES: [&str; 22] = [
    "a", "Z", "0", "9", "-", "_", ".", "/", ":", "*", ">", "+", "#", " ", "'", "\"", "$", "\\",
    "é", "中文", "🎉", "\u{2028}",
];
/// What a name is spelled from when its system admits any text at all, control characters
/// included.
const ANY_PIECES: [&str; 26] = [
    "a", "Z", "0", "9", "-", "_", ".", "/", ":", "*", ">", "+", "#", " ", "'", "\"", "$", "\\",
    "é", "中文", "🎉", "\u{2028}", "\n", "\t", "\0", "\r",
];
/// What a Redis channel pattern is spelled from: any text a channel holds, and every character
/// Redis's glob reads specially, the brackets of a set, its negation and range marks, the
/// one-byte wildcard and an escaped star included.
const REDIS_PATTERN_PIECES: [&str; 32] = [
    "a", "Z", "0", "9", "-", "_", ".", "/", ":", "*", ">", "+", "#", " ", "'", "\"", "$", "\\",
    "é", "中文", "🎉", "\u{2028}", "\n", "\t", "\0", "\r", "?", "[", "]", "^", "[a-z]", "\\*",
];
/// What a NATS queue group, or a token of a NATS subject apart from its separator and wildcards,
/// is spelled from: no whitespace and no control character.
const SOLID_PIECES: [&str; 13] = [
    "a", "Z", "0", "9", "-", "_", "/", ":", "$", "\\", "'", "é", "中文",
];
/// What one level of an MQTT topic filter is spelled from apart from its wildcards: anything but
/// the level separator, the wildcards and NUL.
const MQTT_LEVEL_PIECES: [&str; 13] = [
    "a", "Z", "0", "9", " ", "-", "_", ".", "$", "'", "é", "中文", "\n",
];
/// What a local Pulsar topic, or a level of a qualified one, is spelled from: no `/`, no
/// whitespace and no control character.
const PULSAR_LEVEL_PIECES: [&str; 11] = ["a", "Z", "0", "9", "-", "_", ".", ":", "$", "é", "中文"];

impl Arbitrary<'_> {
    /// A Kafka topic: 1 to 249 bytes of letters, digits, `.`, `_` and `-`, never `.` or `..`.
    pub fn kafka_topic_name(&mut self) -> KafkaTopicName {
        let mut text = self.pieces_text(KAFKA_TOPIC_PIECES, KAFKA_TOPIC_BYTES);
        if text == "." || text == ".." {
            text.push('a');
        }
        KafkaTopicName::parse(&text).assured("a generated Kafka topic follows the topic rule")
    }

    /// A Kafka consumer group: 1 to 255 bytes without control characters.
    pub fn kafka_consumer_group_name(&mut self) -> KafkaConsumerGroupName {
        let text = self.pieces_text(PRINTABLE_PIECES, SHORT_NAME_BYTES);
        KafkaConsumerGroupName::parse(&text)
            .assured("a generated consumer group holds no control character within the bound")
    }

    /// A Pulsar topic: a local name, or one qualified by its domain, tenant and namespace.
    pub fn pulsar_topic_name(&mut self) -> PulsarTopicName {
        let text = if self.entropy.flag() {
            let domain = self.entropy.pick(["persistent", "non-persistent"]);
            let tenant = self.pieces_text(PULSAR_LEVEL_PIECES, LEVEL_BYTES);
            let namespace = self.pieces_text(PULSAR_LEVEL_PIECES, LEVEL_BYTES);
            let topic = self.pieces_text(PULSAR_LEVEL_PIECES, LEVEL_BYTES);
            format!("{domain}://{tenant}/{namespace}/{topic}")
        } else {
            self.pieces_text(PULSAR_LEVEL_PIECES, LONG_NAME_BYTES)
        };
        PulsarTopicName::parse(&text).assured("a generated Pulsar topic follows the topic rule")
    }

    /// A Pulsar subscription: 1 to 255 bytes without control characters.
    pub fn pulsar_subscription_name(&mut self) -> PulsarSubscriptionName {
        let text = self.pieces_text(PRINTABLE_PIECES, SHORT_NAME_BYTES);
        PulsarSubscriptionName::parse(&text)
            .assured("a generated subscription holds no control character within the bound")
    }

    /// A RabbitMQ queue: 1 to 255 bytes of any text.
    pub fn rabbitmq_queue_name(&mut self) -> RabbitMqQueueName {
        let text = self.pieces_text(ANY_PIECES, SHORT_NAME_BYTES);
        RabbitMqQueueName::parse(&text).assured("a generated queue name is within the bound")
    }

    /// An SQS queue: letters, digits, `-` and `_`, where a FIFO queue's name ends in `.fifo`, 80
    /// bytes at most with the suffix.
    pub fn sqs_queue_name(&mut self) -> SqsQueueName {
        let fifo = self.entropy.flag();
        let base_bytes = if fifo {
            SQS_QUEUE_BYTES
                .checked_sub(SQS_FIFO_SUFFIX.len())
                .assured("the FIFO suffix is shorter than the queue bound")
        } else {
            SQS_QUEUE_BYTES
        };
        let mut text = self.pieces_text(SQS_QUEUE_PIECES, base_bytes);
        if fifo {
            text.push_str(SQS_FIFO_SUFFIX);
        }
        SqsQueueName::parse(&text).assured("a generated SQS queue follows the queue rule")
    }

    /// A Redis channel: 1 to 1,024 bytes of any text.
    pub fn redis_channel_name(&mut self) -> RedisChannelName {
        let text = self.pieces_text(ANY_PIECES, LONG_NAME_BYTES);
        RedisChannelName::parse(&text).assured("a generated channel is within the bound")
    }

    /// A Redis channel pattern: 1 to 1,024 bytes of any text, glob characters included.
    pub fn redis_channel_pattern(&mut self) -> RedisChannelPattern {
        let text = self.pieces_text(REDIS_PATTERN_PIECES, LONG_NAME_BYTES);
        RedisChannelPattern::parse(&text).assured("a generated pattern is within the bound")
    }

    /// A NATS subject: one token up to the subject's bound, or several short ones, where a token
    /// may be the one-token wildcard `*` and the last may be the remaining-tokens wildcard `>`.
    pub fn nats_subject_name(&mut self) -> NatsSubjectName {
        let text = if self.entropy.flag() {
            self.pieces_text(SOLID_PIECES, LONG_NAME_BYTES)
        } else {
            let count = self.levels();
            let mut tokens = Vec::with_capacity(count);
            for index in 0..count {
                let last = index
                    .checked_add(1)
                    .verified("an index below the level count is below usize::MAX")
                    == count;
                let token = match self.entropy.byte() % 6 {
                    0 => "*".to_string(),
                    1 if last => ">".to_string(),
                    _ => self.pieces_text(SOLID_PIECES, LEVEL_BYTES),
                };
                tokens.push(token);
            }
            tokens.join(".")
        };
        NatsSubjectName::parse(&text).assured("a generated subject follows the subject rule")
    }

    /// A NATS queue group: 1 to 255 bytes without whitespace or control characters.
    pub fn nats_queue_group_name(&mut self) -> NatsQueueGroupName {
        let text = self.pieces_text(SOLID_PIECES, SHORT_NAME_BYTES);
        NatsQueueGroupName::parse(&text)
            .assured("a generated queue group holds no whitespace or control character")
    }

    /// An MQTT topic filter: one level up to the filter's bound, or several short ones, where a
    /// level may be empty or the one-level wildcard `+`, and the last may be the remaining-levels
    /// wildcard `#`.
    pub fn mqtt_topic_filter(&mut self) -> MqttTopicFilter {
        let text = if self.entropy.flag() {
            self.pieces_text(MQTT_LEVEL_PIECES, MQTT_TOPIC_FILTER_BYTES)
        } else {
            let count = self.levels();
            let mut levels = Vec::with_capacity(count);
            for index in 0..count {
                let last = index
                    .checked_add(1)
                    .verified("an index below the level count is below usize::MAX")
                    == count;
                let level = match self.entropy.byte() % 7 {
                    0 => "+".to_string(),
                    1 if last => "#".to_string(),
                    2 => String::new(),
                    _ => self.pieces_text(MQTT_LEVEL_PIECES, LEVEL_BYTES),
                };
                levels.push(level);
            }
            // Two levels or more join with at least one separator, so the filter is never empty.
            levels.join("/")
        };
        MqttTopicFilter::parse(&text).assured("a generated filter follows the filter rule")
    }

    /// How many levels or tokens a filter or subject of several holds: two or more.
    fn levels(&mut self) -> usize {
        let extra = self.entropy.count(
            MOST_LEVELS
                .checked_sub(2)
                .assured("a filter of several levels holds at most six"),
        );
        extra
            .checked_add(2)
            .verified("at most four extra levels stay far below usize::MAX")
    }

    /// Text of 1 through `max` bytes assembled from `pieces`, whose first is one byte. Its length
    /// is drawn first and lands on the bound as often as anywhere, and a piece that would pass the
    /// length is replaced by the first.
    fn pieces_text<const N: usize>(&mut self, pieces: [&'static str; N], max: usize) -> String {
        let max = u64::try_from(max).assured("a name bound fits in u64");
        let length = self.entropy.boundary_biased(1..=max);
        let length = usize::try_from(length).verified("the length is at most the usize bound");
        let mut text = String::with_capacity(length);
        while text.len() < length {
            let room = length
                .checked_sub(text.len())
                .verified("the loop runs only while the text is shorter than its length");
            let mut piece = self.entropy.pick(pieces);
            if piece.len() > room {
                piece = pieces[0];
            }
            text.push_str(piece);
        }
        text
    }
}

impl Arbitrary<'_> {
    /// A case-preserving MongoDB collection within the declared namespace budget.
    pub fn mongodb_collection_name(&mut self) -> MongoDbCollectionName {
        if self.entropy.flag() {
            return MongoDbCollectionName::parse("a").assured("one letter is a collection name");
        }
        // Reserve the nonempty database and its dot, then the two framing letters. Framing
        // ensures the generated text can never enter a reserved system namespace.
        let body = self.pieces_text(
            ["a", "Z", "0", "9", " ", ".", "_", "-", "~", "é", "中文"],
            MONGODB_NAMESPACE_MAX_BYTES - 4,
        );
        MongoDbCollectionName::parse(&format!("C{body}z"))
            .assured("the collection alphabet excludes NUL and dollar signs within its budget")
    }

    /// A ClickHouse table has one or two nonempty case-preserving identifier parts.
    pub fn clickhouse_table_name(&mut self) -> ClickHouseTableName {
        let pieces = ["a", "Z", "0", "9", "_", "-", "~"];
        let text = if self.entropy.flag() {
            let database = self.pieces_text(pieces, 64);
            let table = self.pieces_text(pieces, 63);
            format!("{database}.{table}")
        } else {
            self.pieces_text(pieces, 128)
        };
        ClickHouseTableName::parse(&text)
            .assured("the generated ClickHouse locator has one or two nonempty parts")
    }

    /// An external table identifier within its current lexical bound, with both cases.
    pub fn emitter_table_name<N: ExternalName>(&mut self) -> N {
        let text = self.pieces_text(["a", "Z", "0", "9", ".", "_", "-", "~"], 128);
        N::parse(&text)
            .assured("the generator uses the identifier alphabet within the declared bound")
    }

    /// A MySQL table or database/table path, reaching the complete 128-byte lexical bound.
    pub fn mysql_table_name(&mut self) -> MySqlTableName {
        self.table_path()
    }

    /// A Postgres table or schema/table path, reaching the complete 128-byte lexical bound.
    pub fn postgres_table_name(&mut self) -> PostgresTableName {
        self.table_path()
    }

    /// A table or qualified table path of one or two nonempty parts, reaching the complete
    /// 128-byte lexical bound.
    fn table_path<N: ExternalName>(&mut self) -> N {
        const PIECES: [&str; 7] = ["a", "Z", "0", "9", "_", "-", "~"];
        let text = if self.entropy.flag() {
            // At most 126 bytes leave a dot and one nonempty table part inside 128 bytes.
            let database = self.pieces_text(PIECES, 126);
            let remaining = 127_usize
                .checked_sub(database.len())
                .assured("a database part is at most 126 bytes, leaving a nonempty table part");
            let table = self.pieces_text(PIECES, remaining);
            format!("{database}.{table}")
        } else {
            self.pieces_text(PIECES, 128)
        };
        N::parse(&text)
            .assured("one or two nonempty identifier parts fit the table path's lexical bound")
    }

    /// A publication topic reaches the MQTT byte bound without subscription wildcards or NUL.
    pub fn mqtt_publication_topic_name(&mut self) -> MqttTopicName {
        let text = self.pieces_text(
            [
                "a", "Z", "0", "9", " ", "-", "_", ".", "$", "'", "é", "中文", "\n", "/",
            ],
            MQTT_TOPIC_FILTER_BYTES,
        );
        MqttTopicName::parse(&text)
            .assured("the publication alphabet contains neither NUL nor wildcards")
    }

    /// A publication subject has concrete nonempty tokens and no subscription wildcards.
    pub fn nats_publication_subject_name(&mut self) -> NatsPublishSubjectName {
        let text = self.pieces_text(SOLID_PIECES, LONG_NAME_BYTES);
        NatsPublishSubjectName::parse(&text)
            .assured("one nonempty subject token contains neither whitespace nor wildcards")
    }
}
