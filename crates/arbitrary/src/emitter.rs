//! Emitters: the sink they publish to, how they publish, and the payload they send.

use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::*;

use crate::{Arbitrary, Domain, route::RouteShape};

/// The most items a generated value mapping or conflict target holds.
const ITEMS: usize = 3;

impl Arbitrary<'_> {
    /// An emitter publishing to any sink, in a publishing mode and with a body that sink accepts,
    /// batching where the sink requires it and optionally where it allows it.
    pub fn create_emitter(&mut self) -> CreateEmitter {
        let kinds = EmitSinkKind::all().collect::<Vec<_>>();
        let count = NonZeroUsize::new(kinds.len()).assured("the vocabulary declares sinks");
        let kind = kinds[self.entropy.index(count)];
        self.create_emitter_to(kind)
    }

    /// An emitter publishing to a sink of the requested connector and entity.
    pub fn create_emitter_to(&mut self, kind: EmitSinkKind) -> CreateEmitter {
        let sink = self.emit_sink_of(kind);
        let body = self.emitter_body(&sink);
        let construction_shape = match body {
            EmitterBody::Codec { .. } | EmitterBody::Client => RouteShape::Transforming,
            EmitterBody::WithoutBody => RouteShape::FilterAndInvoke,
            EmitterBody::Values => RouteShape::FilterOnly,
        };
        // Plexus writes no batching clause where batching is unavailable, as for an HTTP emitter,
        // whose requests carry one record each. The vocabulary can still hold one, which a stored
        // form must carry back, so its domain may draw one.
        let batch = match sink.batch_legality() {
            EmitterBatchLegality::Required { .. } => Some(self.batch_policy()),
            EmitterBatchLegality::Unavailable { .. } if self.domain == Domain::Nspl => None,
            EmitterBatchLegality::Optional { .. } | EmitterBatchLegality::Unavailable { .. } => {
                if self.entropy.flag() {
                    Some(self.batch_policy())
                } else {
                    None
                }
            }
        };
        CreateEmitter {
            name: self.name(),
            from: self.processor_inputs(true),
            body,
            sink: Box::new(sink),
            batch,
            flush_policy: self.flush_policy(),
            error_policies: self.error_policies(),
            mode: self.ack_mode(),
            construction: self.route_construction(construction_shape),
            materialized_state: self.materialized_state(),
        }
    }

    fn emitter_body(&mut self, sink: &EmitSink) -> EmitterBody {
        // A client consumer receives native columns built against its schema, never a payload.
        if let EmitSink::Client(_) = sink {
            return EmitterBody::Client;
        }
        if sink.requires_codec() {
            return EmitterBody::Codec { codec: self.name() };
        }
        if let EmitSink::Http(_) = sink {
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
        let max_size = PayloadSizeLimit::new(
            NonZeroU64::new(count).verified("the range above starts at one"),
            unit,
        )
        .verified("the count is at most the largest the unit's byte count allows");
        EmitterBatchPolicy {
            max_messages,
            max_size,
        }
    }

    /// A sink of the requested connector and entity, with every clause it declares.
    ///
    /// The match is exhaustive, so a new pair does not compile until the generator is taught to
    /// build it.
    pub fn emit_sink_of(&mut self, kind: EmitSinkKind) -> EmitSink {
        match kind {
            EmitSinkKind::Http(HttpSinkEntity::Request) => {
                EmitSink::Http(HttpSink::Request(HttpRequestSink {
                    mode: self.http_request_publication(),
                    client: self.name(),
                    method: self.expression(),
                    path: self.expression(),
                }))
            }
            EmitSinkKind::Kafka(KafkaSinkEntity::Topic) => {
                EmitSink::Kafka(KafkaSink::Topic(KafkaTopicSink {
                    mode: self.kafka_topic_publication(),
                    client: self.name(),
                    topic: self.kafka_topic_name(),
                }))
            }
            EmitSinkKind::Pulsar(PulsarSinkEntity::Topic) => {
                EmitSink::Pulsar(PulsarSink::Topic(PulsarTopicSink {
                    mode: self.pulsar_topic_publication(),
                    client: self.name(),
                    topic: self.pulsar_topic_name(),
                }))
            }
            EmitSinkKind::RabbitMq(RabbitMqSinkEntity::Queue) => {
                EmitSink::RabbitMq(RabbitMqSink::Queue(RabbitMqQueueSink {
                    mode: self.rabbitmq_queue_publication(),
                    client: self.name(),
                    queue: self.rabbitmq_queue_name(),
                }))
            }
            EmitSinkKind::Redis(RedisSinkEntity::PubSub) => {
                EmitSink::Redis(RedisSink::PubSub(RedisPubSubSink {
                    mode: self.redis_pubsub_publication(),
                    client: self.name(),
                    channel: self.redis_channel_name(),
                }))
            }
            EmitSinkKind::Mqtt(MqttSinkEntity::Topic) => {
                EmitSink::Mqtt(MqttSink::Topic(MqttTopicSink {
                    mode: self.mqtt_topic_publication(),
                    client: self.name(),
                    topic: self.mqtt_publication_topic_name(),
                }))
            }
            EmitSinkKind::Nats(NatsSinkEntity::Subject) => {
                EmitSink::Nats(NatsSink::Subject(NatsSubjectSink {
                    mode: self.nats_subject_publication(),
                    client: self.name(),
                    subject: self.nats_publication_subject_name(),
                }))
            }
            EmitSinkKind::ZeroMq(ZeroMqSinkEntity::Push) => {
                EmitSink::ZeroMq(ZeroMqSink::Push(ZeroMqPushSink {
                    mode: self.zeromq_push_publication(),
                    client: self.name(),
                }))
            }
            EmitSinkKind::Sqs(SqsSinkEntity::Queue) => {
                EmitSink::Sqs(SqsSink::Queue(SqsQueueSink {
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
            EmitSinkKind::Sentry(SentrySinkEntity::Event) => {
                EmitSink::Sentry(SentrySink::Event(SentryEventSink {
                    mode: self.sentry_event_publication(),
                    client: self.name(),
                }))
            }
            EmitSinkKind::Syslog(SyslogSinkEntity::Collector) => {
                EmitSink::Syslog(SyslogSink::Collector(SyslogCollectorSink {
                    mode: self.syslog_collector_publication(),
                    client: self.name(),
                }))
            }
            EmitSinkKind::Otel(OtelSinkEntity::Logs) => EmitSink::from(OtelLogsSink {
                mode: self.otel_logs_publication(),
                client: self.name(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::Otel(OtelSinkEntity::Traces) => EmitSink::from(OtelTracesSink {
                mode: self.otel_traces_publication(),
                client: self.name(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::Otel(OtelSinkEntity::Metrics) => EmitSink::from(OtelMetricsSink {
                mode: self.otel_metrics_publication(),
                client: self.name(),
                metric: self.otel_metric(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::ClickHouse(ClickHouseSinkEntity::Table) => {
                EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
                    mode: self.clickhouse_table_publication(),
                    client: self.name(),
                    table: self.emitter_table_name(),
                    values: self.value_mappings(1),
                }))
            }
            EmitSinkKind::Postgres(PostgresSinkEntity::Table) => {
                let client = self.name();
                let table = self.emitter_table_name();
                let values = self.value_mappings(1);
                let conflict_action = self.postgres_conflict_action(&values);
                EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
                    mode: self.postgres_table_publication(),
                    client,
                    table,
                    values,
                    conflict_action,
                }))
            }
            EmitSinkKind::MySql(MySqlSinkEntity::Table) => {
                EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
                    mode: self.mysql_table_publication(),
                    client: self.name(),
                    table: self.emitter_table_name(),
                    values: self.value_mappings(1),
                    conflict_action: self.entropy.pick([
                        MySqlConflictAction::None,
                        MySqlConflictAction::DoNothing,
                        MySqlConflictAction::DoUpdate,
                    ]),
                }))
            }
            EmitSinkKind::MongoDb(MongoDbSinkEntity::Collection) => {
                let values = self.value_mappings(1);
                let conflict_action = self.mongodb_conflict_action(&values);
                EmitSink::MongoDb(MongoDbSink::Collection(MongoDbCollectionSink {
                    mode: self.mongodb_collection_publication(),
                    client: self.name(),
                    collection: self.emitter_table_name(),
                    values,
                    conflict_action,
                }))
            }
            EmitSinkKind::Client(ClientSinkEntity::Consumer) => {
                EmitSink::Client(ClientSink::Consumer(ClientConsumerSink {
                    mode: self.client_consumer_publication(),
                    schema: self.name(),
                }))
            }
            EmitSinkKind::Iceberg(IcebergSinkEntity::Table) => {
                EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                    mode: self.iceberg_table_publication(),
                    client: self.name(),
                    table: self.emitter_table_name(),
                    values: self.value_mappings(1),
                    location: self.string(),
                    catalog: IcebergCatalog::Rest {
                        client: self.name(),
                    },
                    commit_each: self.duration(),
                    max_commit_size: self.byte_size(),
                }))
            }
        }
    }

    /// The mappings every OTEL signal shares, with an optional instrumentation scope.
    fn otel_mappings(&mut self) -> OtelMappings {
        let values = self.value_mappings(1);
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
    /// NSPL reads a `DO UPDATE` only while a mapped column stays outside its target to be updated,
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
                if self.domain == Domain::Nspl {
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
