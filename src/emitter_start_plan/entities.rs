//! Typed emitter entity plans and their resolved client binding.
//!
//! Layer: decisions.
//! - **Owns.** Entity-specific plan data and the conversion from declared to resolved clients.
//! - **Depends on.** Vocabulary values, connector policy values and the parent planner's types.
//! - **Must not know.** Runtime tasks, I/O, locks, placement or connector lifecycle execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns typed emitter entity plans")
)]

use super::*;

/// Declares the plan of an emitter that connects through one client, together with the step that binds
/// that client to the configuration the host resolved for it.
macro_rules! single_client_emitter_plan {
    ($(#[$doc:meta])* $name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub(crate) struct $name<Config = ResolvedClientConfig> {
            pub(crate) client: EmitterClientSpec<Config>,
            $(pub(crate) $field: $type,)*
        }

        impl $name<DeclaredClientConfig> {
            pub(super) fn resolve_clients<Failure>(
                self,
                resolve: &mut impl FnMut(
                    &EmitterClientSpec<DeclaredClientConfig>,
                ) -> Result<ResolvedClientConfig, Failure>,
            ) -> Result<$name, Failure> {
                let client = self.client.resolve(resolve)?;
                Ok($name {
                    client,
                    $($field: self.$field,)*
                })
            }
        }
    };
}

single_client_emitter_plan! {
    /// An HTTP emitter: the origin its client sends every request to.
    HttpRequestEmitterPlan {
        origin: HttpOrigin,
    }
}

single_client_emitter_plan! {
    /// A Kafka emitter: the topic it produces to, and how it learns that a record was accepted.
    KafkaTopicEmitterPlan {
        topic: nervix_models::KafkaTopicName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A Pulsar emitter: the topic its producer publishes to, and how it learns that a record was
    /// accepted.
    PulsarTopicEmitterPlan {
        topic: nervix_models::PulsarTopicName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A RabbitMQ queue emitter: the queue it publishes to through the default exchange, and how it
    /// learns that a record was accepted.
    RabbitMqQueueEmitterPlan {
        queue: nervix_models::RabbitMqQueueName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A RabbitMQ exchange emitter: the exchange it publishes to, and how it learns that a record
    /// was accepted. The key each record is routed by travels with the record's envelope.
    RabbitMqExchangeEmitterPlan {
        exchange: nervix_models::RabbitMqExchangeName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A Redis emitter: the channel it publishes to, and the bounds of the pool its client shares.
    RedisPubSubEmitterPlan {
        pool: ClientPoolBounds,
        channel: nervix_models::RedisChannelName,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// An MQTT emitter: the topic it publishes to, and the quality of service it publishes at.
    MqttTopicEmitterPlan {
        topic: nervix_models::MqttTopicName,
        mode: MqttPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A NATS emitter: the subject it publishes to, and whether JetStream confirms each record.
    NatsSubjectEmitterPlan {
        subject: nervix_models::NatsPublishSubjectName,
        mode: NatsPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A ZeroMQ emitter, which pushes every record through the socket its client configures.
    ZeroMqPushEmitterPlan {
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A Syslog emitter, which sends every record to the collector its client configures.
    SyslogCollectorEmitterPlan {
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// An SQS emitter: the queue it sends to and the request shape it sends its messages in.
    SqsQueueEmitterPlan {
        queue: nervix_models::SqsQueueName,
        send: nervix_models::SqsSend,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_emitter_plan! {
    /// A Sentry emitter, which sends every record as an event to the project its client names.
    SentryEventEmitterPlan {
        batch: Option<EmitterBatchPolicy>,
        platform: String,
    }
}

single_client_emitter_plan! {
    /// An OpenTelemetry emitter: the signal it exports and the mappings that build each item.
    OtelExportPlan {
        mapping: MappedValuesPlan,
        values: Vec<String>,
        attributes: Vec<String>,
        resource: Vec<OtelResourceAttribute>,
        scope: Option<OtelScope>,
        batch: Option<EmitterBatchPolicy>,
    }
}

/// A logs export has no signal selector.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OtelLogsEmitterPlan<Config = ResolvedClientConfig> {
    pub(crate) export: OtelExportPlan<Config>,
}
/// A traces export has no signal selector.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OtelTracesEmitterPlan<Config = ResolvedClientConfig> {
    pub(crate) export: OtelExportPlan<Config>,
}
/// A metrics export carries its metric declaration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OtelMetricsEmitterPlan<Config = ResolvedClientConfig> {
    pub(crate) export: OtelExportPlan<Config>,
    pub(crate) metric: OtelMetric,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OtelEmitterPlan<Config = ResolvedClientConfig> {
    Logs(OtelLogsEmitterPlan<Config>),
    Traces(OtelTracesEmitterPlan<Config>),
    Metrics(OtelMetricsEmitterPlan<Config>),
}

impl<Config> OtelEmitterPlan<Config> {
    pub(super) fn export(&self) -> &OtelExportPlan<Config> {
        match self {
            Self::Logs(plan) => &plan.export,
            Self::Traces(plan) => &plan.export,
            Self::Metrics(plan) => &plan.export,
        }
    }
}

impl OtelEmitterPlan<DeclaredClientConfig> {
    pub(super) fn resolve_clients<Failure>(
        self,
        resolve: &mut impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<OtelEmitterPlan, Failure> {
        Ok(match self {
            Self::Logs(plan) => OtelEmitterPlan::Logs(OtelLogsEmitterPlan {
                export: plan.export.resolve_clients(resolve)?,
            }),
            Self::Traces(plan) => OtelEmitterPlan::Traces(OtelTracesEmitterPlan {
                export: plan.export.resolve_clients(resolve)?,
            }),
            Self::Metrics(plan) => OtelEmitterPlan::Metrics(OtelMetricsEmitterPlan {
                export: plan.export.resolve_clients(resolve)?,
                metric: plan.metric,
            }),
        })
    }
}

single_client_emitter_plan! {
    /// A ClickHouse emitter: the table it inserts into, the mapping of each column, and the most rows
    /// one insert carries.
    ClickHouseTableEmitterPlan {
        table: nervix_models::ClickHouseTableName,
        mapping: MappedValuesPlan,
        batch: EmitterBatchPolicy,
    }
}

single_client_emitter_plan! {
    /// A Postgres emitter: the table it inserts into, the mapping of each column, what a conflicting
    /// row does, the most rows one insert carries, and the bounds of the pool its client shares.
    PostgresTableEmitterPlan {
        pool: ClientPoolBounds,
        table: nervix_models::PostgresTableName,
        mapping: MappedValuesPlan,
        conflict_action: PostgresConflictAction,
        batch: EmitterBatchPolicy,
    }
}

single_client_emitter_plan! {
    /// A MySQL emitter: the table it inserts into, the mapping of each column, what a conflicting row
    /// does, the most rows one insert carries, and the bounds of the pool its client shares.
    MySqlTableEmitterPlan {
        pool: ClientPoolBounds,
        table: nervix_models::MySqlTableName,
        mapping: MappedValuesPlan,
        conflict_action: MySqlConflictAction,
        batch: EmitterBatchPolicy,
    }
}

single_client_emitter_plan! {
    /// A MongoDB emitter: the collection it writes to, the mapping of each field, what a conflicting
    /// document does, the most documents one write carries, and the bounds of the pool its client
    /// shares.
    MongoDbCollectionEmitterPlan {
        pool: ClientPoolBounds,
        collection: nervix_models::MongoDbCollectionName,
        mapping: MappedValuesPlan,
        conflict_action: MongoDbConflictAction,
        batch: EmitterBatchPolicy,
    }
}
