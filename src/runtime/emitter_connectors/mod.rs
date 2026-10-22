//! The composition root of the emitter host's sink side.
//!
//! Layer: data plane.
//! - **Owns.** Mapping each variant of an emitter's sink plan to its connector crate's constructor,
//!   and pairing the connector it opens with the input the host prepares for its contract. It is
//!   the only place that names every sink crate.
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
use nervix_connector::{
    EmitterStartResult, HttpRequestEmitter, RecordEmitter, RowEmitter, RowRequestEmitter,
};
use nervix_connector_clickhouse::{ClickHouseEmitter, ClickHouseEmitterConfig};
use nervix_connector_http::{HttpEmitter, HttpEmitterConfig};
use nervix_connector_iceberg::{IcebergEmitter, IcebergEmitterConfig};
use nervix_connector_kafka::{KafkaEmitter, KafkaEmitterConfig};
use nervix_connector_mongodb::{MongoDbEmitter, MongoDbEmitterConfig};
use nervix_connector_mqtt::{MqttEmitter, MqttEmitterConfig};
use nervix_connector_mysql::{MySqlEmitter, MySqlEmitterConfig};
use nervix_connector_nats::{NatsEmitter, NatsEmitterConfig};
use nervix_connector_otel::{OtelEmitter, OtelEmitterConfig};
use nervix_connector_postgres::{PostgresEmitter, PostgresEmitterConfig};
use nervix_connector_pulsar::{PulsarEmitter, PulsarEmitterConfig};
use nervix_connector_rabbitmq::{RabbitMqEmitter, RabbitMqEmitterConfig};
use nervix_connector_redis::{RedisEmitter, RedisEmitterConfig};
use nervix_connector_sentry::{SentryEmitter, SentryEmitterConfig};
use nervix_connector_sqs::{SqsEmitter, SqsEmitterConfig};
use nervix_connector_syslog::{SyslogEmitter, SyslogEmitterConfig};
use nervix_connector_zeromq::{ZeroMqEmitter, ZeroMqEmitterConfig};
use nervix_models::EmitterBatchPolicy;

use super::{
    emitter_http_requests::{HttpRequestBody, PreparedRequestEmitter},
    pooled_emitter_clients::PooledEmitterClient,
    *,
};

/// The composition root of the sink side, and the only place that names every sink crate.
///
/// Each variant of an emitter's sink plan maps to its crate's constructor, and the connector that
/// constructor opens is paired with what the host prepares its input with: a record sink with the
/// codec its records are encoded by, a row sink or a row request sink with the projection that maps
/// its columns, and an HTTP sink with the body its requests carry.
pub(super) struct EmitterConnectorStarter;

impl EmitterConnectorStarter {
    /// Rejects, while the emitter is built, a client configuration its connector can already tell
    /// will never open.
    ///
    /// Only Syslog reads everything it needs from its configuration alone, so it is the only sink
    /// checked before its task starts; every other sink reports its client when it opens.
    pub(super) fn check_client_config(plan: &EmitterStartPlan) -> EmitterRuntimeResult<()> {
        match &plan.connector {
            EmitterConnectorPlan::Syslog(connector) => {
                SyslogEmitter::check_client_config(&connector.client.config.entries)
                    .change_context(EmitterRuntimeError::InvalidSinkConfig)
            }
            EmitterConnectorPlan::Client(_)
            | EmitterConnectorPlan::Http(_)
            | EmitterConnectorPlan::Kafka(_)
            | EmitterConnectorPlan::Pulsar(_)
            | EmitterConnectorPlan::RabbitMq(_)
            | EmitterConnectorPlan::Redis(_)
            | EmitterConnectorPlan::Mqtt(_)
            | EmitterConnectorPlan::Nats(_)
            | EmitterConnectorPlan::ZeroMq(_)
            | EmitterConnectorPlan::Sqs(_)
            | EmitterConnectorPlan::Sentry(_)
            | EmitterConnectorPlan::Otel(_)
            | EmitterConnectorPlan::ClickHouse(_)
            | EmitterConnectorPlan::Postgres(_)
            | EmitterConnectorPlan::MySql(_)
            | EmitterConnectorPlan::MongoDb(_)
            | EmitterConnectorPlan::Iceberg(_) => Ok(()),
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
        let label = plan.connector.label();
        let batch = plan.connector.batch();
        let connector: Box<dyn EmitterConnector> = match &plan.connector {
            EmitterConnectorPlan::Client(connector) => {
                Self::client(connector, context, output_schema, plan)?
            }
            EmitterConnectorPlan::Http(connector) => Self::http(connector, context, codec)?,
            EmitterConnectorPlan::Kafka(connector) => {
                Self::kafka(connector, context, codec, label, batch)?
            }
            EmitterConnectorPlan::Pulsar(connector) => {
                Self::pulsar(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::RabbitMq(connector) => {
                Self::rabbitmq(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Redis(connector) => {
                Self::redis(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Mqtt(connector) => {
                Self::mqtt(connector, context, codec, label, batch, plan)?
            }
            EmitterConnectorPlan::Nats(connector) => {
                Self::nats(connector, context, codec, label, batch, plan).await?
            }
            EmitterConnectorPlan::ZeroMq(connector) => {
                Self::zeromq(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Syslog(connector) => {
                Self::syslog(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Sqs(connector) => {
                Self::sqs(connector, context, codec, label, batch).await?
            }
            EmitterConnectorPlan::Sentry(connector) => {
                Self::sentry(connector, context, codec, label, batch)?
            }
            EmitterConnectorPlan::Otel(connector) => Self::otel(connector, context, input_schema)?,
            EmitterConnectorPlan::ClickHouse(connector) => {
                Self::clickhouse(connector, context, input_schema)?
            }
            EmitterConnectorPlan::Postgres(connector) => {
                Self::postgres(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::MySql(connector) => {
                Self::mysql(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::MongoDb(connector) => {
                Self::mongodb(connector, context, input_schema).await?
            }
            EmitterConnectorPlan::Iceberg(connector) => {
                Self::iceberg(connector, context, input_schema).await?
            }
        };
        Ok(connector)
    }

    /// Pairs a record sink with the codec the host encodes its records with, and with the `BATCH`
    /// clause whose payloads it publishes when the emitter declares one.
    ///
    /// The registry requires `ENCODE USING` on exactly the emitters that publish encoded records, so a
    /// record sink always receives its codec here.
    fn record<T>(
        label: &str,
        codec: Option<&Arc<CompiledCodec>>,
        batch: Option<EmitterBatchPolicy>,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RecordEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let Some(codec) = codec else {
            return Err(Report::new(EmitterRuntimeError::InvalidSinkConfig)
                .attach_printable(format!("{label} emitter requires an encoding codec")));
        };
        let connector: Box<dyn EmitterConnector> = Box::new(EncodedRecordEmitter {
            connector: Box::new(connector),
            codec: codec.clone(),
            batch,
        });
        Ok(connector)
    }

    /// Pairs an HTTP sink with the body the host prepares each of its requests with: the bytes of
    /// the codec an emitter encodes its records with, or no content for an emitter declared
    /// `WITHOUT BODY`, which names no codec.
    fn http_request<T>(
        codec: Option<&Arc<CompiledCodec>>,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: HttpRequestEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let body = match codec {
            Some(codec) => HttpRequestBody::Encoded(codec.clone()),
            None => HttpRequestBody::Absent,
        };
        let connector: Box<dyn EmitterConnector> =
            Box::new(PreparedRequestEmitter::new(Box::new(connector), body));
        Ok(connector)
    }

    /// Pairs a row sink with the projection whose mapped columns it writes.
    fn row<T>(
        projection: MappedValuesProjection,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RowEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let connector: Box<dyn EmitterConnector> =
            Box::new(MappedRowEmitter::new(Box::new(connector), projection));
        Ok(connector)
    }

    /// Pairs a row request sink with the projection whose mapped columns it prepares requests from.
    fn row_request<T>(
        projection: MappedValuesProjection,
        started: EmitterStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>>
    where
        T: RowRequestEmitter + 'static,
    {
        let connector = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let connector: Box<dyn EmitterConnector> =
            Box::new(MappedRequestEmitter::new(Box::new(connector), projection));
        Ok(connector)
    }

    /// Compiles one row sink's `VALUES` mapping before the sink it feeds is opened.
    ///
    /// A mapping that cannot compile never produces a column, so the emitter reports the failure
    /// the same way it reports a client it could not open and recompiles on its next attempt.
    fn projection(
        init: MappedValuesProjectionInit<'_>,
    ) -> EmitterRuntimeResult<MappedValuesProjection> {
        MappedValuesProjection::compile(init).change_context(EmitterRuntimeError::InitializeSink)
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
    /// open is rejected while the emitter is built, and every other sink reports its client only
    /// when it opens.
    #[test]
    fn only_a_syslog_client_that_could_never_open_is_rejected_before_the_emitter_starts() {
        let valid = plan(EmitterConnectorPlan::Syslog(SyslogEmitterPlan {
            client: client(&[("protocol", "udp"), ("addr", "127.0.0.1:5514")]),
            batch: None,
        }));
        assert!(EmitterConnectorStarter::check_client_config(&valid).is_ok());

        let unopenable = plan(EmitterConnectorPlan::Syslog(SyslogEmitterPlan {
            client: client(&[("protocol", "tcp"), ("addr", "missing-port")]),
            batch: None,
        }));
        let Err(error) = EmitterConnectorStarter::check_client_config(&unopenable) else {
            panic!("a Syslog address without a port must fail the client check")
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::InvalidSinkConfig
        );
        let reason = emitter_error_message(&error);
        assert!(
            reason.contains("addr"),
            "the rejection names the key the connector refused: {reason}"
        );

        let unchecked = plan(EmitterConnectorPlan::ZeroMq(ZeroMqEmitterPlan {
            client: client(&[]),
            batch: None,
        }));
        assert!(EmitterConnectorStarter::check_client_config(&unchecked).is_ok());
    }

    /// A connector decides why its own configuration is unusable, so the host states only that the
    /// sink could not be initialized and reports the connector's message as the reason it is
    /// unavailable.
    #[test]
    fn a_connector_start_failure_leaves_the_emitter_unavailable_with_its_own_message() {
        let started = EmitterConnectorStarter::record::<NatsEmitter>(
            "nats",
            None,
            None,
            Err(
                Report::new(EmitterOpenError::InvalidConfiguration { connector: "NATS" })
                    .attach_printable("missing NATS client config key 'servers'"),
            ),
        );

        let Err(error) = started else {
            panic!("a start failure must leave the sink unavailable")
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::InitializeSink
        );
        assert_eq!(
            emitter_error_message(&error),
            "missing NATS client config key 'servers'"
        );
    }
}
