//! The changes an `ALTER` makes in place: one part of a route's construction, one part of the
//! body of an ingestor's or an emitter's entity, and one clause of an emitter's envelope.
//!
//! Plexus spells a change only as its entity's own grammar writes it: a `SET QUIESCE` the entity
//! declares, a `SET CLIENT` where it reads or publishes through a client, an envelope clause or
//! property it declares, and a construction change its body can hold. The vocabulary holds any
//! change, which a stored form must carry back, so its domain draws from all of them.

use std::num::NonZeroUsize;

use meticulous::OptionExt as _;
use nervix_models::*;

use crate::{Arbitrary, Domain, route::RouteShape};

/// The most items a generated list inside a change holds.
const ITEMS: usize = 3;

/// The highest invocation position a generated `DROP INVOKE` names.
const INVOCATION_POSITIONS: u64 = 4;

/// One change a construction takes in place.
#[derive(Clone, Copy)]
enum ConstructionForm {
    SetInherit,
    DropInherit,
    AddSet,
    DropSet,
    AddInvoke,
    DropInvoke,
    SetWhere,
    DropWhere,
}

impl ConstructionForm {
    /// The changes a construction of `shape` can hold: a filter alone, a filter and invocations,
    /// or every part.
    fn of(shape: RouteShape) -> &'static [Self] {
        match shape {
            RouteShape::FilterOnly => &[Self::SetWhere, Self::DropWhere],
            RouteShape::FilterAndInvoke => &[
                Self::AddInvoke,
                Self::DropInvoke,
                Self::SetWhere,
                Self::DropWhere,
            ],
            RouteShape::Transforming | RouteShape::SetOnly => &[
                Self::SetInherit,
                Self::DropInherit,
                Self::AddSet,
                Self::DropSet,
                Self::AddInvoke,
                Self::DropInvoke,
                Self::SetWhere,
                Self::DropWhere,
            ],
        }
    }
}

impl Arbitrary<'_> {
    /// One change a construction of `shape` takes in place.
    pub fn construction_alteration(&mut self, shape: RouteShape) -> ConstructionAlteration {
        let forms = ConstructionForm::of(shape);
        let count = NonZeroUsize::new(forms.len()).assured("every shape holds a filter change");
        match forms[self.entropy.index(count)] {
            ConstructionForm::SetInherit => ConstructionAlteration::SetInherit {
                inheritance: self.inheritance(),
            },
            ConstructionForm::DropInherit => ConstructionAlteration::DropInherit,
            ConstructionForm::AddSet => {
                let assignment = self
                    .assignments(1, true)
                    .into_iter()
                    .next()
                    .assured("one assignment was asked for at least");
                ConstructionAlteration::AddSet { assignment }
            }
            ConstructionForm::DropSet => ConstructionAlteration::DropSet {
                target: self.assignment_target(),
            },
            ConstructionForm::AddInvoke => ConstructionAlteration::AddInvoke {
                invocation: self.invocation(),
            },
            ConstructionForm::DropInvoke => {
                let position = self.entropy.boundary_biased(1..=INVOCATION_POSITIONS);
                ConstructionAlteration::DropInvoke {
                    position: std::num::NonZeroU64::new(position)
                        .assured("the range above starts at one"),
                }
            }
            ConstructionForm::SetWhere => ConstructionAlteration::SetWhere {
                where_clause: self.expression(),
            },
            ConstructionForm::DropWhere => ConstructionAlteration::DropWhere,
        }
    }

    /// The field an assignment writes: bare, or in a message, output or branch scope.
    fn assignment_target(&mut self) -> AssignmentTarget {
        AssignmentTarget {
            scope: self.entropy.pick([
                AssignmentTargetScope::Bare,
                AssignmentTargetScope::Message,
                AssignmentTargetScope::Output,
                AssignmentTargetScope::Branch,
            ]),
            field: self.expression_name(),
        }
    }

    /// A quiesce mode an ingestor of `kind` may be set to: one its entity declares in Plexus, and
    /// any in the vocabulary.
    pub fn declared_quiesce_mode(&mut self, kind: IngestorEntityKind) -> IngestQuiesceMode {
        if self.domain != Domain::Plexus {
            return self.quiesce_mode();
        }
        let declared = kind.capabilities().quiesce;
        let count = NonZeroUsize::new(declared.len())
            .assured("every entity declares at least one quiesce mode");
        let quiesce = declared[self.entropy.index(count)].kind;
        self.quiesce_mode_of(quiesce)
    }

    /// A change to the body of a `CLIENT PRODUCER` ingestor.
    pub fn client_producer_change(&mut self) -> ClientProducerIngestorChange {
        ClientProducerIngestorChange::Mode(self.client_producer_source().mode)
    }

    /// A change to the body of an `HTTP REQUEST` ingestor.
    pub fn http_request_change(&mut self) -> HttpRequestIngestorChange {
        let source = self.http_request_source();
        match self.entropy.byte() % 7 {
            0 => HttpRequestIngestorChange::Method(source.method),
            1 => HttpRequestIngestorChange::Headers(source.headers),
            2 => HttpRequestIngestorChange::DropHeaders,
            3 => HttpRequestIngestorChange::MaxBodySize(self.payload_size_limit()),
            4 => HttpRequestIngestorChange::DropMaxBodySize,
            5 => HttpRequestIngestorChange::Every(source.every),
            _ => HttpRequestIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of an `HTTP ENDPOINT` ingestor.
    pub fn http_endpoint_change(&mut self) -> HttpEndpointIngestorChange {
        HttpEndpointIngestorChange::Mode(self.http_endpoint_source().mode)
    }

    /// A change to the body of a `KAFKA TOPIC` ingestor.
    pub fn kafka_topic_change(&mut self) -> KafkaTopicIngestorChange {
        let source = self.kafka_topic_source();
        match self.entropy.byte() % 4 {
            0 => KafkaTopicIngestorChange::Topic(source.topic),
            1 => KafkaTopicIngestorChange::OffsetBy(source.offset_mode),
            2 => KafkaTopicIngestorChange::StartAt(self.ingest_start_position()),
            _ => KafkaTopicIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `PULSAR TOPIC` ingestor.
    pub fn pulsar_topic_change(&mut self) -> PulsarTopicIngestorChange {
        let source = self.pulsar_topic_source();
        match self.entropy.byte() % 5 {
            0 => PulsarTopicIngestorChange::Topic(source.topic),
            1 => PulsarTopicIngestorChange::Subscription {
                name: source.subscription.name,
                ownership: source.subscription.ownership,
            },
            2 => PulsarTopicIngestorChange::StartAt(self.ingest_start_position()),
            3 => PulsarTopicIngestorChange::Type(self.pulsar_subscription_type()),
            _ => PulsarTopicIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of an `MQTT TOPIC` ingestor.
    pub fn mqtt_topic_change(&mut self) -> MqttTopicIngestorChange {
        let source = self.mqtt_topic_source();
        match self.entropy.byte() % 4 {
            0 => MqttTopicIngestorChange::Topic(source.topic),
            1 => MqttTopicIngestorChange::Session(source.session),
            2 => MqttTopicIngestorChange::Qos(source.qos),
            _ => MqttTopicIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `NATS SUBJECT` ingestor.
    pub fn nats_subject_change(&mut self) -> NatsSubjectIngestorChange {
        let source = self.nats_subject_source();
        match self.entropy.byte() % 3 {
            0 => NatsSubjectIngestorChange::Subject(source.subject),
            1 => NatsSubjectIngestorChange::QueueGroup(source.queue_group),
            _ => NatsSubjectIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `NATS STREAM` ingestor.
    pub fn nats_stream_change(&mut self) -> NatsStreamIngestorChange {
        let source = self.nats_stream_source();
        match self.entropy.byte() % 3 {
            0 => NatsStreamIngestorChange::Stream(source.stream),
            1 => NatsStreamIngestorChange::Consumer(source.consumer),
            _ => NatsStreamIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `RABBITMQ QUEUE` ingestor.
    pub fn rabbitmq_queue_change(&mut self) -> RabbitMqQueueIngestorChange {
        let source = self.rabbitmq_queue_source();
        if self.entropy.flag() {
            RabbitMqQueueIngestorChange::Queue(source.queue)
        } else {
            RabbitMqQueueIngestorChange::Mode(source.mode)
        }
    }

    /// A change to the body of a `REDIS PUBSUB` ingestor.
    pub fn redis_pubsub_change(&mut self) -> RedisPubSubIngestorChange {
        let source = self.redis_pubsub_source();
        if self.entropy.flag() {
            RedisPubSubIngestorChange::Subscription(source.subscription)
        } else {
            RedisPubSubIngestorChange::Mode(source.mode)
        }
    }

    /// A change to the body of a `PROMETHEUS QUERY` ingestor.
    pub fn prometheus_query_change(&mut self) -> PrometheusQueryIngestorChange {
        let source = self.prometheus_query_source();
        match self.entropy.byte() % 5 {
            0 => PrometheusQueryIngestorChange::Query(source.query),
            1 => PrometheusQueryIngestorChange::MaxBodySize(self.payload_size_limit()),
            2 => PrometheusQueryIngestorChange::DropMaxBodySize,
            3 => PrometheusQueryIngestorChange::Every(source.every),
            _ => PrometheusQueryIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `ZEROMQ PULL` ingestor.
    pub fn zeromq_pull_change(&mut self) -> ZeroMqPullIngestorChange {
        ZeroMqPullIngestorChange::Mode(self.zeromq_pull_source().mode)
    }

    /// A change to the body of an `SQS QUEUE` ingestor.
    pub fn sqs_queue_change(&mut self) -> SqsQueueIngestorChange {
        let source = self.sqs_queue_source();
        if self.entropy.flag() {
            SqsQueueIngestorChange::Queue(source.queue)
        } else {
            SqsQueueIngestorChange::Mode(source.mode)
        }
    }

    /// A change to the body of a `WEBSOCKETS CONNECTION` ingestor.
    pub fn websockets_connection_change(&mut self) -> WebsocketsConnectionIngestorChange {
        let source = self.websockets_connection_source();
        match self.entropy.byte() % 3 {
            0 => WebsocketsConnectionIngestorChange::Headers(source.headers),
            1 => WebsocketsConnectionIngestorChange::DropHeaders,
            _ => WebsocketsConnectionIngestorChange::Mode(source.mode),
        }
    }

    /// A change to the body of a `WEBSOCKETS ENDPOINT` ingestor.
    pub fn websockets_endpoint_change(&mut self) -> WebsocketsEndpointIngestorChange {
        WebsocketsEndpointIngestorChange::Mode(self.websockets_endpoint_source().mode)
    }

    /// A change to the body of a `SYSLOG LISTENER` ingestor.
    pub fn syslog_listener_change(&mut self) -> SyslogListenerIngestorChange {
        SyslogListenerIngestorChange::Mode(self.syslog_listener_source().mode)
    }

    /// A change to the body of an emitter of `kind`: one part of a body of that entity, drawn
    /// from a whole body so every part is one a statement of the entity can write.
    ///
    /// The match is exhaustive, so a new pair does not compile until the generator is taught to
    /// change it.
    pub fn emitter_change(&mut self, kind: EmitterEntityKind) -> EmitterChange {
        match self.emitter_entity_of(kind) {
            EmitterEntity::Client(ClientEmitter::Consumer(entity)) => {
                ClientConsumerEmitterChange::Mode(entity.mode).into()
            }
            EmitterEntity::Http(HttpEmitter::Request(entity)) => {
                self.http_request_emitter_change(entity)
            }
            EmitterEntity::Kafka(KafkaEmitter::Topic(entity)) => {
                self.kafka_topic_emitter_change(entity)
            }
            EmitterEntity::Pulsar(PulsarEmitter::Topic(entity)) => {
                self.pulsar_topic_emitter_change(entity)
            }
            EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(entity)) => {
                self.rabbitmq_queue_emitter_change(entity)
            }
            EmitterEntity::RabbitMq(RabbitMqEmitter::Exchange(entity)) => {
                self.rabbitmq_exchange_emitter_change(entity)
            }
            EmitterEntity::Redis(RedisEmitter::PubSub(entity)) => {
                self.redis_pubsub_emitter_change(entity)
            }
            EmitterEntity::Mqtt(MqttEmitter::Topic(entity)) => {
                self.mqtt_topic_emitter_change(entity)
            }
            EmitterEntity::Nats(NatsEmitter::Subject(entity)) => {
                self.nats_subject_emitter_change(entity)
            }
            EmitterEntity::ZeroMq(ZeroMqEmitter::Push(entity)) => {
                ZeroMqPushEmitterChange::Mode(entity.mode).into()
            }
            EmitterEntity::Sqs(SqsEmitter::Queue(entity)) => self.sqs_queue_emitter_change(entity),
            EmitterEntity::Sentry(SentryEmitter::Event(entity)) => {
                self.sentry_event_emitter_change(entity)
            }
            EmitterEntity::Syslog(SyslogEmitter::Collector(entity)) => {
                SyslogCollectorEmitterChange::Mode(entity.mode).into()
            }
            EmitterEntity::Otel(entity) => self.otel_emitter_change(entity),
            EmitterEntity::ClickHouse(ClickHouseEmitter::Table(entity)) => {
                self.clickhouse_table_emitter_change(entity)
            }
            EmitterEntity::Postgres(PostgresEmitter::Table(entity)) => {
                self.postgres_table_emitter_change(entity)
            }
            EmitterEntity::MySql(MySqlEmitter::Table(entity)) => {
                self.mysql_table_emitter_change(entity)
            }
            EmitterEntity::MongoDb(MongoDbEmitter::Collection(entity)) => {
                self.mongodb_collection_emitter_change(entity)
            }
            EmitterEntity::Iceberg(IcebergEmitter::Table(entity)) => {
                self.iceberg_table_emitter_change(entity)
            }
        }
    }

    /// A change to the body of `entity`, an `HTTP REQUEST` emitter.
    fn http_request_emitter_change(&mut self, entity: HttpRequestEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => HttpRequestEmitterChange::Method(entity.method),
            1 => HttpRequestEmitterChange::Path(entity.path),
            _ => HttpRequestEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `KAFKA TOPIC` emitter.
    fn kafka_topic_emitter_change(&mut self, entity: KafkaTopicEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            KafkaTopicEmitterChange::Topic(entity.topic)
        } else {
            KafkaTopicEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, a `PULSAR TOPIC` emitter.
    fn pulsar_topic_emitter_change(&mut self, entity: PulsarTopicEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            PulsarTopicEmitterChange::Topic(entity.topic)
        } else {
            PulsarTopicEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, a `RABBITMQ QUEUE` emitter.
    fn rabbitmq_queue_emitter_change(&mut self, entity: RabbitMqQueueEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            RabbitMqQueueEmitterChange::Queue(entity.queue)
        } else {
            RabbitMqQueueEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, a `RABBITMQ EXCHANGE` emitter.
    fn rabbitmq_exchange_emitter_change(
        &mut self,
        entity: RabbitMqExchangeEmitter,
    ) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => RabbitMqExchangeEmitterChange::Exchange(entity.exchange),
            1 => RabbitMqExchangeEmitterChange::RoutingKey(entity.routing_key),
            _ => RabbitMqExchangeEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `REDIS PUBSUB` emitter.
    fn redis_pubsub_emitter_change(&mut self, entity: RedisPubSubEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            RedisPubSubEmitterChange::Channel(entity.channel)
        } else {
            RedisPubSubEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, an `MQTT TOPIC` emitter.
    fn mqtt_topic_emitter_change(&mut self, entity: MqttTopicEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => MqttTopicEmitterChange::Topic(entity.topic),
            1 => MqttTopicEmitterChange::Qos(entity.qos),
            _ => MqttTopicEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `NATS SUBJECT` emitter.
    fn nats_subject_emitter_change(&mut self, entity: NatsSubjectEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            NatsSubjectEmitterChange::Subject(entity.subject)
        } else {
            NatsSubjectEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, a `SENTRY EVENT` emitter.
    fn sentry_event_emitter_change(&mut self, entity: SentryEventEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => SentryEventEmitterChange::Platform(self.string()),
            1 => SentryEventEmitterChange::DropPlatform,
            _ => SentryEventEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `CLICKHOUSE TABLE` emitter.
    fn clickhouse_table_emitter_change(&mut self, entity: ClickHouseTableEmitter) -> EmitterChange {
        let change = if self.entropy.flag() {
            ClickHouseTableEmitterChange::Table(entity.table)
        } else {
            ClickHouseTableEmitterChange::Mode(entity.mode)
        };
        change.into()
    }

    /// A change to the body of `entity`, a `POSTGRES TABLE` emitter.
    fn postgres_table_emitter_change(&mut self, entity: PostgresTableEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => PostgresTableEmitterChange::Table(entity.table),
            1 => PostgresTableEmitterChange::OnConflict(entity.conflict_action),
            _ => PostgresTableEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `MYSQL TABLE` emitter.
    fn mysql_table_emitter_change(&mut self, entity: MySqlTableEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => MySqlTableEmitterChange::Table(entity.table),
            1 => MySqlTableEmitterChange::OnConflict(entity.conflict_action),
            _ => MySqlTableEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, a `MONGODB COLLECTION` emitter.
    fn mongodb_collection_emitter_change(
        &mut self,
        entity: MongoDbCollectionEmitter,
    ) -> EmitterChange {
        let change = match self.entropy.byte() % 3 {
            0 => MongoDbCollectionEmitterChange::Collection(entity.collection),
            1 => MongoDbCollectionEmitterChange::OnConflict(entity.conflict_action),
            _ => MongoDbCollectionEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, an `SQS QUEUE` emitter.
    fn sqs_queue_emitter_change(&mut self, entity: SqsQueueEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 5 {
            0 => SqsQueueEmitterChange::Queue(entity.queue),
            1 => SqsQueueEmitterChange::FifoGroup(
                entity.fifo_group.unwrap_or_else(|| self.expression()),
            ),
            2 => SqsQueueEmitterChange::DropFifoGroup,
            3 => SqsQueueEmitterChange::Send(entity.send),
            _ => SqsQueueEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, an `ICEBERG TABLE` emitter.
    fn iceberg_table_emitter_change(&mut self, entity: IcebergTableEmitter) -> EmitterChange {
        let change = match self.entropy.byte() % 6 {
            0 => IcebergTableEmitterChange::Table(entity.table),
            1 => IcebergTableEmitterChange::Namespace(entity.namespace),
            2 => IcebergTableEmitterChange::Location(entity.location),
            3 => IcebergTableEmitterChange::Catalog(entity.catalog),
            4 => IcebergTableEmitterChange::CommitEach {
                commit_each: entity.commit_each,
                max_commit_size: entity.max_commit_size,
            },
            _ => IcebergTableEmitterChange::Mode(entity.mode),
        };
        change.into()
    }

    /// A change to the body of `entity`, an emitter of one OTEL signal.
    fn otel_emitter_change(&mut self, entity: OtelEmitter) -> EmitterChange {
        let mappings_change = self.entropy.flag();
        match entity {
            OtelEmitter::Logs(entity) => {
                if mappings_change {
                    OtelLogsEmitterChange::Mappings(self.otel_mapping_change()).into()
                } else {
                    OtelLogsEmitterChange::Mode(entity.mode).into()
                }
            }
            OtelEmitter::Traces(entity) => {
                if mappings_change {
                    OtelTracesEmitterChange::Mappings(self.otel_mapping_change()).into()
                } else {
                    OtelTracesEmitterChange::Mode(entity.mode).into()
                }
            }
            OtelEmitter::Metrics(entity) => match self.entropy.byte() % 3 {
                0 => OtelMetricsEmitterChange::Metric(entity.metric).into(),
                1 => OtelMetricsEmitterChange::Mappings(self.otel_mapping_change()).into(),
                _ => OtelMetricsEmitterChange::Mode(entity.mode).into(),
            },
        }
    }

    /// A change to what an OTEL emitter's exports carry beside their values. A statement writes at
    /// least one attribute or resource entry wherever it writes the clause.
    fn otel_mapping_change(&mut self) -> OtelMappingChange {
        match self.entropy.byte() % 6 {
            0 => OtelMappingChange::Attributes(self.mapped_entries(false)),
            1 => OtelMappingChange::DropAttributes,
            2 => OtelMappingChange::Resource(self.mapped_entries(true)),
            3 => OtelMappingChange::DropResource,
            4 => {
                let name = self.string();
                let version = if self.entropy.flag() {
                    Some(self.string())
                } else {
                    None
                };
                OtelMappingChange::Scope(OtelScope { name, version })
            }
            _ => OtelMappingChange::DropScope,
        }
    }

    /// Between one and three named entries, each an expression or, where `literal`, a literal.
    fn mapped_entries(&mut self, literal: bool) -> Vec<EmitterValueMapping> {
        let count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("an entry list holds at least one"));
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let expression = if literal {
                Expression::Literal(self.literal())
            } else {
                self.expression()
            };
            entries.push(EmitterValueMapping {
                column: self.string(),
                expression,
            });
        }
        entries
    }

    /// An envelope clause an emitter of `kind` may set: one its entity declares in Plexus, and any
    /// in the vocabulary. `None` where the entity declares none.
    pub fn envelope_clause_value(
        &mut self,
        kind: EmitterEntityKind,
    ) -> Option<EnvelopeClauseValue> {
        let clause = self.envelope_clause_kind(kind)?;
        let value = match clause {
            EnvelopeClauseKind::Key => EnvelopeClauseValue::Key(self.expression()),
            EnvelopeClauseKind::PartitionFromBranch => EnvelopeClauseValue::PartitionFromBranch,
            EnvelopeClauseKind::Persistent => EnvelopeClauseValue::Persistent,
            EnvelopeClauseKind::Mandatory => EnvelopeClauseValue::Mandatory,
            EnvelopeClauseKind::Retain => EnvelopeClauseValue::Retain,
            EnvelopeClauseKind::Timestamp => EnvelopeClauseValue::Timestamp(self.expression()),
            EnvelopeClauseKind::DeliverAfter => EnvelopeClauseValue::DeliverAfter(self.duration()),
            EnvelopeClauseKind::Delay => EnvelopeClauseValue::Delay(self.duration()),
            EnvelopeClauseKind::ReplyTo => EnvelopeClauseValue::ReplyTo(self.expression()),
            EnvelopeClauseKind::MessageId => EnvelopeClauseValue::MessageId(self.expression()),
            EnvelopeClauseKind::DeduplicationId => {
                EnvelopeClauseValue::DeduplicationId(self.expression())
            }
        };
        Some(value)
    }

    /// An envelope clause kind an emitter of `kind` may write: one its entity declares in Plexus,
    /// and any in the vocabulary. `None` where the entity declares none.
    pub fn envelope_clause_kind(&mut self, kind: EmitterEntityKind) -> Option<EnvelopeClauseKind> {
        let clauses: Vec<EnvelopeClauseKind> = if self.domain == Domain::Plexus {
            kind.capabilities().envelope.to_vec()
        } else {
            <EnvelopeClauseKind as strum::IntoEnumIterator>::iter().collect()
        };
        let count = NonZeroUsize::new(clauses.len())?;
        Some(clauses[self.entropy.index(count)])
    }

    /// The properties an emitter of `kind` may set, at least one: keys its entity declares in
    /// Plexus, each at most once, and any keys in the vocabulary. `None` where the entity declares
    /// no property.
    pub fn envelope_properties(
        &mut self,
        kind: EmitterEntityKind,
    ) -> Option<Vec<EnvelopeProperty>> {
        if self.domain != Domain::Plexus {
            let mut properties = Vec::new();
            let count = self
                .entropy
                .positive_count(NonZeroUsize::new(ITEMS).assured("a block sets a property"));
            for _ in 0..count {
                properties.push(EnvelopeProperty {
                    name: self.string(),
                    expression: self.expression(),
                });
            }
            return Some(properties);
        }
        let declared = kind.capabilities().properties;
        let first = declared.first()?;
        let mut properties = vec![EnvelopeProperty {
            name: first.name.to_string(),
            expression: self.expression(),
        }];
        for property in declared.iter().skip(1) {
            if self.entropy.flag() {
                properties.push(EnvelopeProperty {
                    name: property.name.to_string(),
                    expression: self.expression(),
                });
            }
        }
        Some(properties)
    }
}
