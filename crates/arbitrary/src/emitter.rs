//! Emitters: the sink they publish to, how they publish, and the payload they send.

use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BatchMessageLimit, ByteSizeUnit, ClickHouseSink, ClickHouseSinkEntity, ClickHouseTableSink,
    ClickHouseValueMapping, ClientConsumerSink, ClientSink, ClientSinkEntity, CreateEmitter,
    EmitSink, EmitSinkKind, EmitterBatchLegality, EmitterBatchPolicy, EmitterBody,
    EmitterPublishingMode, Expression, HttpRequestSink, HttpSink, HttpSinkEntity, IcebergCatalog,
    IcebergSink, IcebergSinkEntity, IcebergStorageBackend, IcebergTableSink, KafkaSink,
    KafkaSinkEntity, KafkaTopicSink, MongoDbCollectionSink, MongoDbConflictAction, MongoDbSink,
    MongoDbSinkEntity, MqttSink, MqttSinkEntity, MqttTopicSink, MySqlConflictAction, MySqlSink,
    MySqlSinkEntity, MySqlTableSink, NatsSink, NatsSinkEntity, NatsSubjectSink,
    OtelAggregationTemporality, OtelLogsSink, OtelMappings, OtelMetric, OtelMetricKind,
    OtelMetricsSink, OtelScope, OtelSinkEntity, OtelTracesSink, PayloadSizeLimit,
    PostgresConflictAction, PostgresSink, PostgresSinkEntity, PostgresTableSink, PulsarSink,
    PulsarSinkEntity, PulsarTopicSink, RabbitMqQueueSink, RabbitMqSink, RabbitMqSinkEntity,
    RedisPubSubSink, RedisSink, RedisSinkEntity, SentryEventSink, SentrySink, SentrySinkEntity,
    SqsFifoGroup, SqsQueueSink, SqsSink, SqsSinkEntity, SyslogCollectorSink, SyslogSink,
    SyslogSinkEntity, ZeroMqPushSink, ZeroMqSink, ZeroMqSinkEntity,
};

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
        let publishing_mode = self.accepted_publishing_mode(&sink);
        // Plexus writes no batching clause where batching is unavailable, as for an HTTP emitter,
        // whose requests carry one record each. The vocabulary can still hold one, which a stored
        // form must carry back, so its domain may draw one.
        let batch = match sink.batch_legality() {
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
        CreateEmitter {
            name: self.name(),
            from: self.processor_inputs(true),
            body,
            sink: Box::new(sink),
            batch,
            flush_policy: self.flush_policy(),
            error_policies: self.error_policies(),
            publishing_mode,
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

    /// A publishing mode the sink accepts, chosen among every mode the vocabulary defines.
    fn accepted_publishing_mode(&mut self, sink: &EmitSink) -> EmitterPublishingMode {
        let candidates = [
            EmitterPublishingMode::NoAck {
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::BrokerAck {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::MqttQos0 {
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::MqttQos1 {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::MqttQos2 {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::NatsJetStream {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::SqsSingle {
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::SqsBatch {
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::RequestAck {
                retry_policy: self.retry_policy(),
            },
            EmitterPublishingMode::ClientAck {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
        ];
        let mut accepted = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if sink.accepts_publishing_mode(&candidate) {
                accepted.push(candidate);
            }
        }
        let count = NonZeroUsize::new(accepted.len())
            .assured("every sink accepts at least one publishing mode");
        let chosen = self.entropy.index(count);
        accepted.swap_remove(chosen)
    }

    fn batch_policy(&mut self) -> EmitterBatchPolicy {
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
                    client: self.name(),
                    method: self.expression(),
                    path: self.expression(),
                }))
            }
            EmitSinkKind::Kafka(KafkaSinkEntity::Topic) => {
                EmitSink::Kafka(KafkaSink::Topic(KafkaTopicSink {
                    client: self.name(),
                    topic: self.name(),
                }))
            }
            EmitSinkKind::Pulsar(PulsarSinkEntity::Topic) => {
                EmitSink::Pulsar(PulsarSink::Topic(PulsarTopicSink {
                    client: self.name(),
                    topic: self.name(),
                }))
            }
            EmitSinkKind::RabbitMq(RabbitMqSinkEntity::Queue) => {
                EmitSink::RabbitMq(RabbitMqSink::Queue(RabbitMqQueueSink {
                    client: self.name(),
                    queue: self.name(),
                }))
            }
            EmitSinkKind::Redis(RedisSinkEntity::PubSub) => {
                EmitSink::Redis(RedisSink::PubSub(RedisPubSubSink {
                    client: self.name(),
                    channel: self.name(),
                }))
            }
            EmitSinkKind::Mqtt(MqttSinkEntity::Topic) => {
                EmitSink::Mqtt(MqttSink::Topic(MqttTopicSink {
                    client: self.name(),
                    topic: self.name(),
                }))
            }
            EmitSinkKind::Nats(NatsSinkEntity::Subject) => {
                EmitSink::Nats(NatsSink::Subject(NatsSubjectSink {
                    client: self.name(),
                    subject: self.name(),
                }))
            }
            EmitSinkKind::ZeroMq(ZeroMqSinkEntity::Push) => {
                EmitSink::ZeroMq(ZeroMqSink::Push(ZeroMqPushSink {
                    client: self.name(),
                }))
            }
            EmitSinkKind::Sqs(SqsSinkEntity::Queue) => {
                EmitSink::Sqs(SqsSink::Queue(SqsQueueSink {
                    client: self.name(),
                    queue: self.sqs_queue(),
                    fifo_group: match self.entropy.byte() % 3 {
                        0 => None,
                        1 => Some(SqsFifoGroup::FromBranch),
                        _ => Some(SqsFifoGroup::Expression(self.expression())),
                    },
                }))
            }
            EmitSinkKind::Sentry(SentrySinkEntity::Event) => {
                EmitSink::Sentry(SentrySink::Event(SentryEventSink {
                    client: self.name(),
                }))
            }
            EmitSinkKind::Syslog(SyslogSinkEntity::Collector) => {
                EmitSink::Syslog(SyslogSink::Collector(SyslogCollectorSink {
                    client: self.name(),
                }))
            }
            EmitSinkKind::Otel(OtelSinkEntity::Logs) => EmitSink::from(OtelLogsSink {
                client: self.name(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::Otel(OtelSinkEntity::Traces) => EmitSink::from(OtelTracesSink {
                client: self.name(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::Otel(OtelSinkEntity::Metrics) => EmitSink::from(OtelMetricsSink {
                client: self.name(),
                metric: self.otel_metric(),
                mappings: self.otel_mappings(),
            }),
            EmitSinkKind::ClickHouse(ClickHouseSinkEntity::Table) => {
                EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
                    client: self.name(),
                    table: self.name(),
                    values: self.value_mappings(1),
                }))
            }
            EmitSinkKind::Postgres(PostgresSinkEntity::Table) => {
                let client = self.name();
                let table = self.name();
                let values = self.value_mappings(1);
                let conflict_action = self.postgres_conflict_action(&values);
                EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
                    client,
                    table,
                    values,
                    conflict_action,
                }))
            }
            EmitSinkKind::MySql(MySqlSinkEntity::Table) => {
                EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
                    client: self.name(),
                    table: self.name(),
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
                    client: self.name(),
                    collection: self.name(),
                    values,
                    conflict_action,
                }))
            }
            EmitSinkKind::Client(ClientSinkEntity::Consumer) => {
                EmitSink::Client(ClientSink::Consumer(ClientConsumerSink {
                    schema: self.name(),
                }))
            }
            EmitSinkKind::Iceberg(IcebergSinkEntity::Table) => {
                EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                    backend: self.entropy.pick([
                        IcebergStorageBackend::S3,
                        IcebergStorageBackend::Gcs,
                        IcebergStorageBackend::AzureBlob,
                    ]),
                    client: self.name(),
                    table: self.name(),
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

    /// An SQS queue as a sink names it: words and whole numbers joined by hyphens, spelled as
    /// written, optionally a FIFO queue.
    fn sqs_queue(&mut self) -> String {
        let parts = self.bounded_count(1);
        let mut queue = String::new();
        for part in 0..parts {
            if part > 0 {
                queue.push('-');
            }
            if self.entropy.flag() {
                let number = self.entropy.boundary_biased(1..=u64::from(u16::MAX));
                queue.push_str(&number.to_string());
            } else if self.entropy.flag() {
                queue.push_str(&self.name_text().to_ascii_uppercase());
            } else {
                queue.push_str(&self.name_text());
            }
        }
        if self.entropy.flag() {
            queue.push_str(".fifo");
        }
        queue
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
