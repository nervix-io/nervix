//! Emitters: the emitter they publish to, how they publish, and the payload they send.

use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::*;
use strum::IntoEnumIterator as _;

use crate::{Arbitrary, Domain, route::RouteShape};

/// The most items a generated value mapping or conflict target holds.
const ITEMS: usize = 3;

impl Arbitrary<'_> {
    /// A case-preserving catalog namespace, including multiple levels and the length boundary.
    pub fn iceberg_namespace(&mut self) -> IcebergNamespace {
        let text = self.entropy.pick([
            "Analytics".to_string(),
            "Analytics.Events".to_string(),
            "a.B-c.d_e~f".to_string(),
            "N".repeat(128),
        ]);
        IcebergNamespace::try_from(text)
            .verified("all generated catalog namespaces satisfy their rule")
    }

    /// An emitter publishing to any emitter, in a publishing mode and with a body that emitter accepts,
    /// batching where the emitter requires it and optionally where it allows it.
    pub fn create_emitter(&mut self) -> CreateEmitter {
        let kinds = EmitterEntityKind::all().collect::<Vec<_>>();
        let count = NonZeroUsize::new(kinds.len()).assured("the vocabulary declares emitters");
        let kind = kinds[self.entropy.index(count)];
        self.create_emitter_to(kind)
    }

    /// An emitter publishing to an emitter of the requested connector and entity.
    pub fn create_emitter_to(&mut self, kind: EmitterEntityKind) -> CreateEmitter {
        let connector = self.emitter_entity_of(kind);
        let body = self.emitter_body(&connector);
        let construction_shape = match body {
            EmitterBody::Codec { .. } | EmitterBody::Client => RouteShape::Transforming,
            EmitterBody::WithoutBody => RouteShape::FilterAndInvoke,
            EmitterBody::Values => RouteShape::FilterOnly,
        };
        // Plexus writes no batching clause where batching is unavailable, as for an HTTP emitter,
        // whose requests carry one record each. The vocabulary can still hold one, which a stored
        // form must carry back, so its domain may draw one.
        let batch = match connector.batch_legality() {
            EmitterBatchLegality::Required { .. } => Some(self.batch_policy()),
            EmitterBatchLegality::Unavailable { .. } if self.domain == Domain::Plexus => None,
            EmitterBatchLegality::Optional { .. } | EmitterBatchLegality::Unavailable { .. } => {
                if self.entropy.flag() {
                    Some(self.batch_policy())
                } else {
                    None
                }
            }
        };
        let envelope = self.emitter_envelope_of(kind);
        CreateEmitter {
            name: self.name(),
            from: self.processor_inputs(true),
            body,
            envelope: Box::new(envelope),
            connector: Box::new(connector),
            batch,
            flush_policy: self.flush_policy(),
            error_policies: self.error_policies(),
            mode: self.ack_mode(),
            construction: self.route_construction(construction_shape),
            materialized_state: self.materialized_state(),
        }
    }

    /// The envelope an emitter of `kind` writes.
    ///
    /// Plexus writes only the clauses and properties the entity declares. The vocabulary holds any
    /// envelope, which a stored form must carry back, so its domain draws every clause kind and
    /// properties of any name.
    pub fn emitter_envelope_of(&mut self, kind: EmitterEntityKind) -> EmitterEnvelope {
        let declared = kind.capabilities();
        if self.domain == Domain::Plexus {
            return self.emitter_envelope(declared.envelope, declared.properties);
        }
        let clauses = EnvelopeClauseKind::iter().collect::<Vec<_>>();
        let mut envelope = self.emitter_envelope(&clauses, &[]);
        let properties = self.entropy.count(ITEMS);
        for _ in 0..properties {
            envelope.properties.push(EnvelopeProperty {
                name: self.string(),
                expression: self.expression(),
            });
        }
        envelope
    }

    /// An envelope that writes some of `clauses` and sets some of `properties`, each at most once,
    /// as a statement writes them: the clauses in the order their kinds are listed, and the
    /// properties in the order they are declared.
    pub fn emitter_envelope(
        &mut self,
        clauses: &[EnvelopeClauseKind],
        properties: &[PropertyDeclaration],
    ) -> EmitterEnvelope {
        let mut envelope = EmitterEnvelope::default();
        for kind in EnvelopeClauseKind::iter() {
            if !clauses.contains(&kind) || !self.entropy.flag() {
                continue;
            }
            match kind {
                EnvelopeClauseKind::Persistent => envelope.persistent = true,
                EnvelopeClauseKind::Mandatory => envelope.mandatory = true,
                EnvelopeClauseKind::Retain => envelope.retain = true,
                EnvelopeClauseKind::Timestamp => envelope.timestamp = Some(self.expression()),
                EnvelopeClauseKind::DeliverAfter => envelope.deliver_after = Some(self.duration()),
                EnvelopeClauseKind::Delay => envelope.delay = Some(self.duration()),
                EnvelopeClauseKind::ReplyTo => envelope.reply_to = Some(self.expression()),
                EnvelopeClauseKind::MessageId => envelope.message_id = Some(self.expression()),
                EnvelopeClauseKind::DeduplicationId => {
                    envelope.deduplication_id = Some(self.expression());
                }
            }
        }
        for property in properties {
            if !self.entropy.flag() {
                continue;
            }
            envelope.properties.push(EnvelopeProperty {
                name: property.name.to_string(),
                expression: self.expression(),
            });
        }
        envelope
    }

    fn emitter_body(&mut self, connector: &EmitterEntity) -> EmitterBody {
        // A client consumer receives native columns built against its schema, never a payload.
        if let EmitterEntity::Client(_) = connector {
            return EmitterBody::Client;
        }
        if connector.requires_codec() {
            return EmitterBody::Codec { codec: self.name() };
        }
        if let EmitterEntity::Http(_) = connector {
            return if self.entropy.flag() {
                EmitterBody::Codec { codec: self.name() }
            } else {
                EmitterBody::WithoutBody
            };
        }
        EmitterBody::Values
    }

    pub(crate) fn batch_policy(&mut self) -> EmitterBatchPolicy {
        let messages = self
            .entropy
            .boundary_biased(1..=u64::from(BatchMessageLimit::MAX));
        let max_messages =
            BatchMessageLimit::try_from(messages).verified("the range above is the limit's range");
        let max_size = self.payload_size_limit();
        EmitterBatchPolicy {
            max_messages,
            max_size,
        }
    }

    /// A size limit in any unit, from one of the unit to the most a 64-bit byte count holds.
    pub fn payload_size_limit(&mut self) -> PayloadSizeLimit {
        let unit = self.entropy.pick([
            ByteSizeUnit::B,
            ByteSizeUnit::KB,
            ByteSizeUnit::KiB,
            ByteSizeUnit::MB,
            ByteSizeUnit::MiB,
            ByteSizeUnit::GB,
            ByteSizeUnit::GiB,
            ByteSizeUnit::TB,
            ByteSizeUnit::TiB,
        ]);
        let most = u64::MAX / unit.bytes();
        let count = self.entropy.boundary_biased(1..=most);
        PayloadSizeLimit::new(
            NonZeroU64::new(count).verified("the range above starts at one"),
            unit,
        )
        .verified("the count is at most the largest the unit's byte count allows")
    }

    /// A emitter of the requested connector and entity, with every clause it declares.
    ///
    /// The match is exhaustive, so a new pair does not compile until the generator is taught to
    /// build it.
    pub fn emitter_entity_of(&mut self, kind: EmitterEntityKind) -> EmitterEntity {
        match kind {
            EmitterEntityKind::Http(HttpEmitterEntity::Request) => {
                EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                    mode: self.http_request_publication(),
                    client: self.name(),
                    method: self.expression(),
                    path: self.expression(),
                }))
            }
            EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic) => {
                EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
                    mode: self.kafka_topic_publication(),
                    client: self.name(),
                    topic: self.kafka_topic_name(),
                }))
            }
            EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic) => {
                EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
                    mode: self.pulsar_topic_publication(),
                    client: self.name(),
                    topic: self.pulsar_topic_name(),
                }))
            }
            EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Queue) => {
                EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(RabbitMqQueueEmitter {
                    mode: self.rabbitmq_queue_publication(),
                    client: self.name(),
                    queue: self.rabbitmq_queue_name(),
                }))
            }
            EmitterEntityKind::Redis(RedisEmitterEntity::PubSub) => {
                EmitterEntity::Redis(RedisEmitter::PubSub(RedisPubSubEmitter {
                    mode: self.redis_pubsub_publication(),
                    client: self.name(),
                    channel: self.redis_channel_name(),
                }))
            }
            EmitterEntityKind::Mqtt(MqttEmitterEntity::Topic) => {
                EmitterEntity::Mqtt(MqttEmitter::Topic(MqttTopicEmitter {
                    mode: self.mqtt_topic_publication(),
                    client: self.name(),
                    topic: self.mqtt_publication_topic_name(),
                }))
            }
            EmitterEntityKind::Nats(NatsEmitterEntity::Subject) => {
                EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
                    mode: self.nats_subject_publication(),
                    client: self.name(),
                    subject: self.nats_publication_subject_name(),
                }))
            }
            EmitterEntityKind::ZeroMq(ZeroMqEmitterEntity::Push) => {
                EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
                    mode: self.zeromq_push_publication(),
                    client: self.name(),
                }))
            }
            EmitterEntityKind::Sqs(SqsEmitterEntity::Queue) => {
                EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                    mode: self.sqs_queue_publication(),
                    client: self.name(),
                    queue: self.sqs_queue_name(),
                    fifo_group: match self.entropy.byte() % 3 {
                        0 => None,
                        1 => Some(SqsFifoGroup::FromBranch),
                        _ => Some(SqsFifoGroup::Expression(self.expression())),
                    },
                }))
            }
            EmitterEntityKind::Sentry(SentryEmitterEntity::Event) => {
                let platform = if self.entropy.flag() {
                    Some(self.string())
                } else {
                    None
                };
                EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
                    mode: self.sentry_event_publication(),
                    client: self.name(),
                    platform,
                }))
            }
            EmitterEntityKind::Syslog(SyslogEmitterEntity::Collector) => {
                EmitterEntity::Syslog(SyslogEmitter::Collector(SyslogCollectorEmitter {
                    mode: self.syslog_collector_publication(),
                    client: self.name(),
                }))
            }
            EmitterEntityKind::Otel(OtelEmitterEntity::Logs) => {
                EmitterEntity::from(OtelLogsEmitter {
                    mode: self.otel_logs_publication(),
                    client: self.name(),
                    mappings: self.otel_mappings(OTEL_LOG_VALUE_FIELDS, false),
                })
            }
            EmitterEntityKind::Otel(OtelEmitterEntity::Traces) => {
                EmitterEntity::from(OtelTracesEmitter {
                    mode: self.otel_traces_publication(),
                    client: self.name(),
                    mappings: self.otel_mappings(OTEL_TRACE_VALUE_FIELDS, false),
                })
            }
            EmitterEntityKind::Otel(OtelEmitterEntity::Metrics) => {
                let metric = self.otel_metric();
                let fields = match metric.kind {
                    OtelMetricKind::Gauge | OtelMetricKind::Sum { .. } => OTEL_NUMBER_VALUE_FIELDS,
                    OtelMetricKind::Histogram { .. } => OTEL_HISTOGRAM_VALUE_FIELDS,
                };
                let mappings = self.otel_mappings(fields, metric.kind.is_delta());
                EmitterEntity::from(OtelMetricsEmitter {
                    mode: self.otel_metrics_publication(),
                    client: self.name(),
                    metric,
                    mappings,
                })
            }
            EmitterEntityKind::ClickHouse(ClickHouseEmitterEntity::Table) => {
                EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
                    mode: self.clickhouse_table_publication(),
                    client: self.name(),
                    table: self.clickhouse_table_name(),
                    values: self.value_mappings(1),
                }))
            }
            EmitterEntityKind::Postgres(PostgresEmitterEntity::Table) => {
                let client = self.name();
                let table = self.emitter_table_name();
                let values = self.value_mappings(1);
                let conflict_action = self.postgres_conflict_action(&values);
                EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
                    mode: self.postgres_table_publication(),
                    client,
                    table,
                    values,
                    conflict_action,
                }))
            }
            EmitterEntityKind::MySql(MySqlEmitterEntity::Table) => {
                EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
                    mode: self.mysql_table_publication(),
                    client: self.name(),
                    table: self.mysql_table_name(),
                    values: self.value_mappings(1),
                    conflict_action: self.entropy.pick([
                        MySqlConflictAction::None,
                        MySqlConflictAction::DoNothing,
                        MySqlConflictAction::DoUpdate,
                    ]),
                }))
            }
            EmitterEntityKind::MongoDb(MongoDbEmitterEntity::Collection) => {
                let values = self.value_mappings(1);
                let conflict_action = self.mongodb_conflict_action(&values);
                EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
                    mode: self.mongodb_collection_publication(),
                    client: self.name(),
                    collection: self.mongodb_collection_name(),
                    values,
                    conflict_action,
                }))
            }
            EmitterEntityKind::Client(ClientEmitterEntity::Consumer) => {
                EmitterEntity::Client(ClientEmitter::Consumer(ClientConsumerEmitter {
                    mode: self.client_consumer_publication(),
                    schema: self.name(),
                }))
            }
            EmitterEntityKind::Iceberg(IcebergEmitterEntity::Table) => {
                EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
                    mode: self.iceberg_table_publication(),
                    client: self.name(),
                    table: self.emitter_table_name(),
                    values: self.value_mappings(1),
                    location: self.string(),
                    namespace: self.iceberg_namespace(),
                    catalog: IcebergCatalog::Rest {
                        client: self.name(),
                    },
                    commit_each: self.duration(),
                    max_commit_size: self.payload_size_limit(),
                }))
            }
        }
    }

    /// The mappings every OTEL signal shares, with an optional instrumentation scope.
    fn otel_mappings(&mut self, fields: &[OtelValueField], delta: bool) -> OtelMappings {
        let mut values = Vec::new();
        for field in fields {
            if field.presence.is_required(delta) || self.entropy.flag() {
                values.push(EmitterValueMapping {
                    column: field.key.to_owned(),
                    expression: self.expression(),
                });
            }
        }
        let attributes = self.value_mappings(0);
        let resource = self.literal_mappings();
        let scope = if self.entropy.flag() {
            let name = self.string();
            let version = if self.entropy.flag() {
                Some(self.string())
            } else {
                None
            };
            Some(OtelScope { name, version })
        } else {
            None
        };
        OtelMappings {
            values,
            attributes,
            resource,
            scope,
        }
    }

    fn otel_metric(&mut self) -> OtelMetric {
        let temporality = self.entropy.pick([
            OtelAggregationTemporality::Delta,
            OtelAggregationTemporality::Cumulative,
        ]);
        let kind = match self.entropy.byte() % 3 {
            0 => OtelMetricKind::Gauge,
            1 => OtelMetricKind::Sum {
                monotonic: self.entropy.flag(),
                temporality,
            },
            _ => OtelMetricKind::Histogram { temporality },
        };
        OtelMetric {
            name: self.string(),
            unit: self.string(),
            description: if self.entropy.flag() {
                Some(self.string())
            } else {
                None
            },
            kind,
        }
    }

    /// At least `minimum` columns written from expressions, in written order.
    fn value_mappings(&mut self, minimum: usize) -> Vec<ClickHouseValueMapping> {
        let count = self.bounded_count(minimum);
        let mut mappings = Vec::with_capacity(count);
        for _ in 0..count {
            mappings.push(ClickHouseValueMapping {
                column: self.string(),
                expression: self.expression(),
            });
        }
        mappings
    }

    /// OpenTelemetry resource attributes, whose values are constant for the whole stream and so
    /// are written as literals.
    fn literal_mappings(&mut self) -> Vec<ClickHouseValueMapping> {
        let count = self.entropy.count(ITEMS);
        let mut mappings = Vec::with_capacity(count);
        for _ in 0..count {
            mappings.push(ClickHouseValueMapping {
                column: self.string(),
                expression: Expression::Literal(self.literal()),
            });
        }
        mappings
    }

    fn bounded_count(&mut self, minimum: usize) -> usize {
        let extra = ITEMS
            .checked_sub(minimum)
            .assured("a list's minimum is within its bound");
        minimum
            .checked_add(self.entropy.count(extra))
            .verified("the extra count is at most the bound minus the minimum")
    }

    /// A MongoDB conflict action. MongoDB identifies a conflicting document by fields the emitter
    /// writes, so a target names only columns the VALUES map declares, and an update leaves at
    /// least one mapped column outside the target to write.
    fn mongodb_conflict_action(
        &mut self,
        values: &[ClickHouseValueMapping],
    ) -> MongoDbConflictAction {
        let mut seen = BTreeSet::new();
        let mut columns = Vec::new();
        for mapping in values {
            if seen.insert(mapping.column.clone()) {
                columns.push(mapping.column.clone());
            }
        }
        let first = columns
            .first()
            .cloned()
            .assured("the VALUES map declares at least one column");
        match self.entropy.byte() % 3 {
            0 => MongoDbConflictAction::None,
            1 => {
                let mut target = vec![first];
                for column in columns.into_iter().skip(1) {
                    if self.entropy.flag() {
                        target.push(column);
                    }
                }
                MongoDbConflictAction::DoNothing { target }
            }
            _ if columns.len() < 2 => MongoDbConflictAction::DoNothing {
                target: vec![first],
            },
            _ => {
                // The target takes a prefix of the mapped columns that leaves the last one out.
                let most = columns
                    .len()
                    .checked_sub(1)
                    .verified("the arm above required at least two columns");
                let taken = self.entropy.positive_count(
                    NonZeroUsize::new(most).verified("the arm above required at least two columns"),
                );
                let target = columns.into_iter().take(taken).collect();
                MongoDbConflictAction::DoUpdate { target }
            }
        }
    }

    /// A Postgres conflict action. `DO NOTHING` may name no conflict target; `DO UPDATE` needs the
    /// one it updates.
    ///
    /// Plexus reads a `DO UPDATE` only while a mapped column stays outside its target to be updated,
    /// so there the target leaves the first mapped column out. The vocabulary holds any target.
    fn postgres_conflict_action(
        &mut self,
        values: &[ClickHouseValueMapping],
    ) -> PostgresConflictAction {
        match self.entropy.byte() % 3 {
            0 => PostgresConflictAction::None,
            1 => PostgresConflictAction::DoNothing {
                target: self.conflict_target(0),
            },
            _ => {
                let mut target = self.conflict_target(1);
                if self.domain == Domain::Plexus {
                    let updated = &values
                        .first()
                        .assured("the VALUES map declares at least one column")
                        .column;
                    target.retain(|column| column != updated);
                    if target.is_empty() {
                        target.push(format!("{updated}_key"));
                    }
                }
                PostgresConflictAction::DoUpdate { target }
            }
        }
    }

    /// At least `minimum` conflict target columns, each any string.
    fn conflict_target(&mut self, minimum: usize) -> Vec<String> {
        let count = self.bounded_count(minimum);
        let mut target = Vec::with_capacity(count);
        for _ in 0..count {
            target.push(self.string());
        }
        target
    }
}

macro_rules! publication_generators {
    ($($method:ident($arbitrary:ident) -> $Mode:ty = [$($policy:expr),+ $(,)?];)+) => {
        impl Arbitrary<'_> {
            $(fn $method(&mut self) -> $Mode {
                let $arbitrary = self;
                let policies = [$($policy,)+];
                $arbitrary.entropy.pick(policies)
            })+
        }
    };
}
publication_generators! {
    client_consumer_publication(arbitrary) -> ClientConsumerPublishingMode = [ClientConsumerPublishingMode::Ack { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    http_request_publication(arbitrary) -> HttpRequestPublishingMode = [HttpRequestPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    kafka_topic_publication(arbitrary) -> KafkaTopicPublishingMode = [KafkaTopicPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }, KafkaTopicPublishingMode::Ack { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    pulsar_topic_publication(arbitrary) -> PulsarTopicPublishingMode = [PulsarTopicPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }, PulsarTopicPublishingMode::Ack { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    rabbitmq_queue_publication(arbitrary) -> RabbitMqQueuePublishingMode = [RabbitMqQueuePublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }, RabbitMqQueuePublishingMode::Ack { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    redis_pubsub_publication(arbitrary) -> RedisPubSubPublishingMode = [RedisPubSubPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }];
    mqtt_topic_publication(arbitrary) -> MqttTopicPublishingMode = [MqttTopicPublishingMode::Qos0 { retry_policy: arbitrary.retry_policy() }, MqttTopicPublishingMode::Qos1 { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }, MqttTopicPublishingMode::Qos2 { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    nats_subject_publication(arbitrary) -> NatsSubjectPublishingMode = [NatsSubjectPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }, NatsSubjectPublishingMode::JetStream { window: arbitrary.ack_window(), ack_timeout: arbitrary.duration(), retry_policy: arbitrary.retry_policy() }];
    zeromq_push_publication(arbitrary) -> ZeroMqPushPublishingMode = [ZeroMqPushPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }];
    sqs_queue_publication(arbitrary) -> SqsQueuePublishingMode = [SqsQueuePublishingMode::Single { retry_policy: arbitrary.retry_policy() }, SqsQueuePublishingMode::Batch { retry_policy: arbitrary.retry_policy() }];
    sentry_event_publication(arbitrary) -> SentryEventPublishingMode = [SentryEventPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    syslog_collector_publication(arbitrary) -> SyslogCollectorPublishingMode = [SyslogCollectorPublishingMode::NoAck { retry_policy: arbitrary.retry_policy() }];
    otel_logs_publication(arbitrary) -> OtelLogsPublishingMode = [OtelLogsPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    otel_traces_publication(arbitrary) -> OtelTracesPublishingMode = [OtelTracesPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    otel_metrics_publication(arbitrary) -> OtelMetricsPublishingMode = [OtelMetricsPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    clickhouse_table_publication(arbitrary) -> ClickHouseTablePublishingMode = [ClickHouseTablePublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    postgres_table_publication(arbitrary) -> PostgresTablePublishingMode = [PostgresTablePublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    mysql_table_publication(arbitrary) -> MySqlTablePublishingMode = [MySqlTablePublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    mongodb_collection_publication(arbitrary) -> MongoDbCollectionPublishingMode = [MongoDbCollectionPublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
    iceberg_table_publication(arbitrary) -> IcebergTablePublishingMode = [IcebergTablePublishingMode::Ack { retry_policy: arbitrary.retry_policy() }];
}
