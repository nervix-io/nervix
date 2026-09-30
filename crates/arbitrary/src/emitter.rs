//! Emitters: the sink they publish to, how they publish, and the payload they send.

use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BatchMessageLimit, ByteSizeUnit, ClickHouseValueMapping, CreateEmitter, EmitSink,
    EmitterBatchPolicy, EmitterBatchRequirement, EmitterBody, EmitterPublishingMode, Expression,
    IcebergCatalog, IcebergStorageBackend, MongoDbConflictAction, MySqlConflictAction,
    OtelAggregationTemporality, OtelMetric, OtelMetricKind, OtelScope, OtelSignal,
    PayloadSizeLimit, PostgresConflictAction, SqsFifoGroup,
};

use crate::{Arbitrary, Domain, route::RouteShape};

/// The most items a generated value mapping or conflict target holds.
const ITEMS: usize = 3;

/// Every variant of [`EmitSink`], so a property reaches each sink as often as any other and a
/// coverage check can ask for each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum SinkVariant {
    Client,
    Http,
    Kafka,
    Pulsar,
    RabbitMq,
    Redis,
    Mqtt,
    Nats,
    ZeroMq,
    Sqs,
    Sentry,
    Syslog,
    Otel,
    ClickHouse,
    Postgres,
    MySql,
    MongoDb,
    Iceberg,
}

impl SinkVariant {
    /// Every variant, in declaration order.
    pub const ALL: [Self; 18] = [
        Self::Client,
        Self::Http,
        Self::Kafka,
        Self::Pulsar,
        Self::RabbitMq,
        Self::Redis,
        Self::Mqtt,
        Self::Nats,
        Self::ZeroMq,
        Self::Sqs,
        Self::Sentry,
        Self::Syslog,
        Self::Otel,
        Self::ClickHouse,
        Self::Postgres,
        Self::MySql,
        Self::MongoDb,
        Self::Iceberg,
    ];

    /// The variant `sink` is. The match is exhaustive, so a new sink does not compile until the
    /// generator is taught to build it.
    pub fn of(sink: &EmitSink) -> Self {
        match sink {
            EmitSink::Client { .. } => Self::Client,
            EmitSink::Http { .. } => Self::Http,
            EmitSink::Kafka { .. } => Self::Kafka,
            EmitSink::Pulsar { .. } => Self::Pulsar,
            EmitSink::RabbitMq { .. } => Self::RabbitMq,
            EmitSink::Redis { .. } => Self::Redis,
            EmitSink::Mqtt { .. } => Self::Mqtt,
            EmitSink::Nats { .. } => Self::Nats,
            EmitSink::ZeroMq { .. } => Self::ZeroMq,
            EmitSink::Sqs { .. } => Self::Sqs,
            EmitSink::Sentry { .. } => Self::Sentry,
            EmitSink::Syslog { .. } => Self::Syslog,
            EmitSink::Otel { .. } => Self::Otel,
            EmitSink::ClickHouse { .. } => Self::ClickHouse,
            EmitSink::Postgres { .. } => Self::Postgres,
            EmitSink::MySql { .. } => Self::MySql,
            EmitSink::MongoDb { .. } => Self::MongoDb,
            EmitSink::Iceberg { .. } => Self::Iceberg,
        }
    }
}

impl Arbitrary<'_> {
    /// An emitter publishing to any sink, in a publishing mode and with a body that sink accepts,
    /// batching where the sink requires it and optionally where it allows it.
    pub fn create_emitter(&mut self) -> CreateEmitter {
        let count = NonZeroUsize::new(SinkVariant::ALL.len()).assured("there are sinks");
        let variant = SinkVariant::ALL[self.entropy.index(count)];
        self.create_emitter_to(variant)
    }

    /// An emitter publishing to a sink of the requested variant.
    pub fn create_emitter_to(&mut self, variant: SinkVariant) -> CreateEmitter {
        let sink = self.emit_sink_of(variant);
        let body = self.emitter_body(&sink);
        let construction_shape = match body {
            EmitterBody::Codec { .. } | EmitterBody::Client => RouteShape::Transforming,
            EmitterBody::WithoutBody => RouteShape::FilterAndInvoke,
            EmitterBody::Values => RouteShape::FilterOnly,
        };
        let publishing_mode = self.accepted_publishing_mode(&sink);
        // NSPL writes no batching clause for an HTTP emitter, whose requests carry one record
        // each, even though the vocabulary lets its sink batch.
        let batch = match (sink.batch_requirement(), &sink) {
            (EmitterBatchRequirement::Required, _) => Some(self.batch_policy()),
            (EmitterBatchRequirement::Optional, EmitSink::Http { .. })
                if self.domain == Domain::Nspl =>
            {
                None
            }
            (EmitterBatchRequirement::Optional, _) => {
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
        if let EmitSink::Client { .. } = sink {
            return EmitterBody::Client;
        }
        if sink.requires_codec() {
            return EmitterBody::Codec { codec: self.name() };
        }
        if let EmitSink::Http { .. } = sink {
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

    /// A sink of the requested variant, with every clause it declares.
    pub fn emit_sink_of(&mut self, variant: SinkVariant) -> EmitSink {
        match variant {
            SinkVariant::Http => EmitSink::Http {
                client: self.name(),
                method: self.expression(),
                path: self.expression(),
            },
            SinkVariant::Kafka => EmitSink::Kafka {
                client: self.name(),
                topic: self.name(),
            },
            SinkVariant::Pulsar => EmitSink::Pulsar {
                client: self.name(),
                topic: self.name(),
            },
            SinkVariant::RabbitMq => EmitSink::RabbitMq {
                client: self.name(),
                queue: self.name(),
            },
            SinkVariant::Redis => EmitSink::Redis {
                client: self.name(),
                channel: self.name(),
            },
            SinkVariant::Mqtt => EmitSink::Mqtt {
                client: self.name(),
                topic: self.name(),
            },
            SinkVariant::Nats => EmitSink::Nats {
                client: self.name(),
                subject: self.name(),
            },
            SinkVariant::ZeroMq => EmitSink::ZeroMq {
                client: self.name(),
            },
            SinkVariant::Sqs => EmitSink::Sqs {
                client: self.name(),
                queue: self.sqs_queue(),
                fifo_group: match self.entropy.byte() % 3 {
                    0 => None,
                    1 => Some(SqsFifoGroup::FromBranch),
                    _ => Some(SqsFifoGroup::Expression(self.expression())),
                },
            },
            SinkVariant::Sentry => EmitSink::Sentry {
                client: self.name(),
            },
            SinkVariant::Syslog => EmitSink::Syslog {
                client: self.name(),
            },
            SinkVariant::Otel => EmitSink::Otel {
                client: self.name(),
                signal: self.otel_signal(),
                values: self.value_mappings(1),
                attributes: self.value_mappings(0),
                resource: self.literal_mappings(),
                scope: if self.entropy.flag() {
                    Some(OtelScope {
                        name: self.string(),
                        version: if self.entropy.flag() {
                            Some(self.string())
                        } else {
                            None
                        },
                    })
                } else {
                    None
                },
            },
            SinkVariant::ClickHouse => EmitSink::ClickHouse {
                client: self.name(),
                table: self.name(),
                values: self.value_mappings(1),
            },
            SinkVariant::Postgres => EmitSink::Postgres {
                client: self.name(),
                table: self.name(),
                values: self.value_mappings(1),
                // `DO NOTHING` may name no conflict target; `DO UPDATE` needs the one it updates.
                conflict_action: match self.entropy.byte() % 3 {
                    0 => PostgresConflictAction::None,
                    1 => PostgresConflictAction::DoNothing {
                        target: self.conflict_target(0),
                    },
                    _ => PostgresConflictAction::DoUpdate {
                        target: self.conflict_target(1),
                    },
                },
            },
            SinkVariant::MySql => EmitSink::MySql {
                client: self.name(),
                table: self.name(),
                values: self.value_mappings(1),
                conflict_action: self.entropy.pick([
                    MySqlConflictAction::None,
                    MySqlConflictAction::DoNothing,
                    MySqlConflictAction::DoUpdate,
                ]),
            },
            SinkVariant::MongoDb => {
                let values = self.value_mappings(1);
                let conflict_action = self.mongodb_conflict_action(&values);
                EmitSink::MongoDb {
                    client: self.name(),
                    collection: self.name(),
                    values,
                    conflict_action,
                }
            }
            SinkVariant::Client => EmitSink::Client {
                schema: self.name(),
            },
            SinkVariant::Iceberg => EmitSink::Iceberg {
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
            },
        }
    }

    fn otel_signal(&mut self) -> OtelSignal {
        match self.entropy.byte() % 3 {
            0 => OtelSignal::Logs,
            1 => OtelSignal::Traces,
            _ => {
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
                OtelSignal::Metric(OtelMetric {
                    name: self.string(),
                    unit: self.string(),
                    description: if self.entropy.flag() {
                        Some(self.string())
                    } else {
                        None
                    },
                    kind,
                })
            }
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
