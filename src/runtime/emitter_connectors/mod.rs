//! The composition root of the emitter host's connectors.
//!
//! Layer: data plane.
//! - **Owns.** Mapping each variant of an emitter's connector plan to its connector crate's constructor,
//!   and pairing the connector it opens with the input the host prepares for its contract. It is
//!   the only place that names every emitter crate.
//! - **Depends on.** The emitter start plan, the connector crates and their contract, the host's
//!   values projection, record encoding and request preparation, and the pooled client leases.
//! - **Must not know.** When the emitter publishes, how it buffers or retries, or how a batch is
//!   encoded or mapped.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

mod clickhouse;
mod client;
mod http;
mod iceberg;
mod kafka;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod otel;
mod postgres;
mod pulsar;
mod rabbitmq;
mod redis;
mod sentry;
mod sqs;
mod syslog;
mod zeromq;

use error_stack::ResultExt as _;
use nervix_connector::{EmitterStartResult, RecordEmitter, RowEmitter, RowRequestEmitter};
use nervix_connector_clickhouse::{ClickHouseTableEmitter, ClickHouseTableEmitterConfig};
use nervix_connector_http::{HttpRequestEmitter, HttpRequestEmitterConfig};
use nervix_connector_iceberg::{IcebergTableEmitter, IcebergTableEmitterConfig};
use nervix_connector_kafka::{KafkaTopicEmitter, KafkaTopicEmitterConfig};
use nervix_connector_mongodb::{MongoDbCollectionEmitter, MongoDbCollectionEmitterConfig};
use nervix_connector_mqtt::{MqttTopicEmitter, MqttTopicEmitterConfig};
use nervix_connector_mysql::{MySqlTableEmitter, MySqlTableEmitterConfig};
use nervix_connector_nats::{NatsSubjectEmitter, NatsSubjectEmitterConfig};
use nervix_connector_otel::{
    OtelExportConfig, OtelLogsEmitter, OtelLogsEmitterConfig, OtelMetricsEmitter,
    OtelMetricsEmitterConfig, OtelTracesEmitter, OtelTracesEmitterConfig,
};
use nervix_connector_postgres::{PostgresTableEmitter, PostgresTableEmitterConfig};
use nervix_connector_pulsar::{PulsarTopicEmitter, PulsarTopicEmitterConfig};
use nervix_connector_rabbitmq::{
    RabbitMqExchangeEmitter, RabbitMqExchangeEmitterConfig, RabbitMqQueueEmitter,
    RabbitMqQueueEmitterConfig,
};
use nervix_connector_redis::{RedisPubSubEmitter, RedisPubSubEmitterConfig};
use nervix_connector_sentry::{SentryEventEmitter, SentryEventEmitterConfig};
use nervix_connector_sqs::{SqsQueueEmitter, SqsQueueEmitterConfig};
use nervix_connector_syslog::{SyslogCollectorEmitter, SyslogCollectorEmitterConfig};
use nervix_connector_zeromq::{ZeroMqPushEmitter, ZeroMqPushEmitterConfig};
use nervix_models::EmitterBatchPolicy;

use super::{
    emitter_http_requests::{HttpRequestBody, PreparedRequestEmitter},
    pooled_emitter_clients::PooledEmitterClient,
    *,
};

/// The composition root of the emitter side, and the only place that names every emitter crate.
///
/// Each variant of an emitter's connector plan maps to its crate's constructor, and the connector that
/// constructor opens is paired with what the host prepares its input with: a record emitter with the
/// codec its records are encoded by, a row emitter or a row request emitter with the projection that maps
/// its columns, and an HTTP emitter with the body its requests carry.
pub(super) struct EmitterConnectorStarter;

impl EmitterConnectorStarter {
    /// Rejects, while the emitter is built, a client configuration its connector can already tell
    /// will never open.
    ///
    /// Only Syslog reads everything it needs from its configuration alone, so it is the only emitter
    /// checked before its task starts; every other emitter reports its client when it opens.
    pub(super) fn check_client_config(plan: &EmitterStartPlan) -> EmitterRuntimeResult<()> {
        match &plan.connector {
            EmitterConnectorPlan::Syslog(SyslogEmitterPlan::Collector(connector)) => {
                SyslogCollectorEmitter::check_client_config(&connector.client.config.entries)
                    .change_context(EmitterRuntimeError::InvalidConnectorConfig)
            }
            EmitterConnectorPlan::Client(ClientEmitterPlan::Consumer(_))
            | EmitterConnectorPlan::Http(HttpEmitterPlan::Request(_))
            | EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(_))
            | EmitterConnectorPlan::Pulsar(PulsarEmitterPlan::Topic(_))
            | EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Queue(_))
            | EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Exchange(_))
            | EmitterConnectorPlan::Redis(RedisEmitterPlan::PubSub(_))
            | EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(_))
            | EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(_))
            | EmitterConnectorPlan::ZeroMq(ZeroMqEmitterPlan::Push(_))
            | EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(_))
            | EmitterConnectorPlan::Sentry(SentryEmitterPlan::Event(_))
            | EmitterConnectorPlan::Otel(_)
            | EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(_))
            | EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(_))
            | EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(_))
            | EmitterConnectorPlan::MongoDb(MongoDbEmitterPlan::Collection(_))
            | EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(_)) => Ok(()),
        }
    }

    /// Opens the connector `plan` names.
    pub(super) async fn start(
        plan: &EmitterStartPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
        output_schema: &Arc<CompiledSchema>,
        codec: Option<&Arc<CompiledCodec>>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let label = &plan.label;
        let batch = plan.connector.batch();
        let connector: Box<dyn EmitterConnector> = match &plan.connector {
            EmitterConnectorPlan::Client(ClientEmitterPlan::Consumer(connector)) => {
                Self::client(connector, context, output_schema, plan)?
            }
            EmitterConnectorPlan::Http(HttpEmitterPlan::Request(connector)) => {
                Self::http(connector, context, codec)?
            }
            EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(connector)) => {
                Self::kafka(connector, context, codec, label, batch)?
            }
            EmitterConnectorPlan::Pulsar(PulsarEmitterPlan::Topic(connector)) => {
                Self::pulsar(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Queue(connector)) => {
                Self::rabbitmq(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Exchange(connector)) => {
                Self::rabbitmq_exchange(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Redis(RedisEmitterPlan::PubSub(connector)) => {
                Self::redis(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(connector)) => {
                Self::mqtt(connector, context, codec, label, batch, plan)?
            }
            EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(connector)) => {
                Self::nats(connector, context, codec, label, batch, plan).await?
            }
            EmitterConnectorPlan::ZeroMq(ZeroMqEmitterPlan::Push(connector)) => {
                Self::zeromq(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Syslog(SyslogEmitterPlan::Collector(connector)) => {
                Self::syslog(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(connector)) => {
                Self::sqs(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Sentry(SentryEmitterPlan::Event(connector)) => {
                Self::sentry(connector, context, codec, label, batch)?
            }
            EmitterConnectorPlan::Otel(connector) => Self::otel(connector, context, input_schema)?,
            EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(connector)) => {
                Self::clickhouse(connector, context, input_schema)?
            }
            EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(connector)) => {
                Self::postgres(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(connector)) => {
                Self::mysql(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::MongoDb(MongoDbEmitterPlan::Collection(connector)) => {
                Self::mongodb(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) => {
                Self::iceberg(connector, context, input_schema).await?
            }
        };
        Ok(connector)
    }

    /// Pairs a record emitter with the codec the host encodes its records with, and with the `BATCH`
    /// clause whose payloads it publishes when the emitter declares one.
    ///
    /// The registry requires `ENCODE USING` on exactly the emitters that publish encoded records, so a
    /// record emitter always receives its codec here.
    fn record<T>(
        label: &str,
        codec: Option<&Arc<CompiledCodec>>,
        batch: Option<EmitterBatchPolicy>,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RecordEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeConnector)?;
        let Some(codec) = codec else {
            return Err(Report::new(EmitterRuntimeError::InvalidConnectorConfig)
                .attach_printable(format!("{label} emitter requires an encoding codec")));
        };
        let connector: Box<dyn EmitterConnector> = Box::new(EncodedRecordEmitter {
            connector: Box::new(connector),
            codec: codec.clone(),
            batch,
        });
        Ok(connector)
    }

    /// Pairs an HTTP emitter with the body the host prepares each of its requests with: the bytes of
    /// the codec an emitter encodes its records with, or no content for an emitter declared
    /// `WITHOUT BODY`, which names no codec.
    fn http_request<T>(
        codec: Option<&Arc<CompiledCodec>>,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: nervix_connector::HttpRequestEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeConnector)?;
        let body = match codec {
            Some(codec) => HttpRequestBody::Encoded(codec.clone()),
            None => HttpRequestBody::Absent,
        };
        let connector: Box<dyn EmitterConnector> =
            Box::new(PreparedRequestEmitter::new(Box::new(connector), body));
        Ok(connector)
    }

    /// Pairs a row emitter with the projection whose mapped columns it writes.
    fn row<T>(
        projection: MappedValuesProjection,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RowEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeConnector)?;
        let connector: Box<dyn EmitterConnector> =
            Box::new(MappedRowEmitter::new(Box::new(connector), projection));
        Ok(connector)
    }

    /// Pairs a row request emitter with the projection whose mapped columns it prepares requests from.
    fn row_request<T>(
        projection: MappedValuesProjection,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RowRequestEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeConnector)?;
        let connector: Box<dyn EmitterConnector> =
            Box::new(MappedRequestEmitter::new(Box::new(connector), projection));
        Ok(connector)
    }

    /// Compiles one row emitter's `VALUES` mapping before the emitter it feeds is opened.
    ///
    /// A mapping that cannot compile never produces a column, so the emitter reports the failure
    /// the same way it reports a client it could not open and recompiles on its next attempt.
    fn projection(
        init: MappedValuesProjectionInit<'_>,
    ) -> EmitterRuntimeResult<MappedValuesProjection> {
        MappedValuesProjection::compile(init)
            .change_context(EmitterRuntimeError::InitializeConnector)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{EmitterOpenError, ParsedRetryPolicy, ResolvedClientConfig};
    use nervix_models::ClientConfigEntry;

    use super::*;
    use crate::runtime::test_fixtures::named;

    fn plan(connector: EmitterConnectorPlan) -> EmitterStartPlan {
        EmitterStartPlan {
            label: connector.kind().connector().keyword().to_ascii_lowercase(),
            retry_policy: ParsedRetryPolicy {
                backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
            },
            connector,
        }
    }

    fn client(entries: &[(&str, &str)]) -> EmitterClientSpec {
        EmitterClientSpec {
            name: named("collector"),
            config: ResolvedClientConfig {
                entries: entries
                    .iter()
                    .map(|(key, value)| ClientConfigEntry {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                mounts: None,
            },
        }
    }

    /// Syslog reads its transport from its configuration alone, so a configuration it could never
    /// open is rejected while the emitter is built, and every other emitter reports its client only
    /// when it opens.
    #[test]
    fn only_a_syslog_client_that_could_never_open_is_rejected_before_the_emitter_starts() {
        let valid = plan(EmitterConnectorPlan::Syslog(SyslogEmitterPlan::Collector(
            SyslogCollectorEmitterPlan {
                client: client(&[("protocol", "udp"), ("addr", "127.0.0.1:5514")]),
                batch: None,
            },
        )));
        assert!(EmitterConnectorStarter::check_client_config(&valid).is_ok());

        let unopenable = plan(EmitterConnectorPlan::Syslog(SyslogEmitterPlan::Collector(
            SyslogCollectorEmitterPlan {
                client: client(&[("protocol", "tcp"), ("addr", "missing-port")]),
                batch: None,
            },
        )));
        let Err(error) = EmitterConnectorStarter::check_client_config(&unopenable) else {
            panic!("a Syslog address without a port must fail the client check")
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::InvalidConnectorConfig
        );
        let reason = emitter_error_message(&error);
        assert!(
            reason.contains("addr"),
            "the rejection names the key the connector refused: {reason}"
        );

        let unchecked = plan(EmitterConnectorPlan::ZeroMq(ZeroMqEmitterPlan::Push(
            ZeroMqPushEmitterPlan {
                client: client(&[]),
                batch: None,
            },
        )));
        assert!(EmitterConnectorStarter::check_client_config(&unchecked).is_ok());
    }

    /// A connector decides why its own configuration is unusable, so the host states only that the
    /// emitter could not be initialized and reports the connector's message as the reason it is
    /// unavailable.
    #[test]
    fn a_connector_start_failure_leaves_the_emitter_unavailable_with_its_own_message() {
        let started = EmitterConnectorStarter::record::<NatsSubjectEmitter>(
            "nats",
            None,
            None,
            Err(
                Report::new(EmitterOpenError::InvalidConfiguration { connector: "NATS" })
                    .attach_printable("missing NATS client config key 'servers'"),
            ),
        );

        let Err(error) = started else {
            panic!("a start failure must leave the emitter unavailable")
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::InitializeConnector
        );
        assert_eq!(
            emitter_error_message(&error),
            "missing NATS client config key 'servers'"
        );
    }
}
