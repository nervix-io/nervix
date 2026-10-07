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

use error_stack::ResultExt as _;
use nervix_connector::{HttpRequestSink, RecordSink, RowRequestSink, RowSink, SinkStartResult};
use nervix_connector_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use nervix_connector_http::{HttpSink, HttpSinkConfig};
use nervix_connector_iceberg::{IcebergSink, IcebergSinkConfig};
use nervix_connector_kafka::{KafkaSink, KafkaSinkConfig};
use nervix_connector_mongodb::{MongoDbSink, MongoDbSinkConfig};
use nervix_connector_mqtt::{MqttSink, MqttSinkConfig};
use nervix_connector_mysql::{MySqlSink, MySqlSinkConfig};
use nervix_connector_nats::{NatsSink, NatsSinkConfig};
use nervix_connector_otel::{OtelSink, OtelSinkConfig};
use nervix_connector_postgres::{PostgresSink, PostgresSinkConfig};
use nervix_connector_pulsar::{PulsarSink, PulsarSinkConfig};
use nervix_connector_rabbitmq::{RabbitMqSink, RabbitMqSinkConfig};
use nervix_connector_redis::{RedisSink, RedisSinkConfig};
use nervix_connector_sentry::{SentrySink, SentrySinkConfig};
use nervix_connector_sqs::{SqsSink, SqsSinkConfig};
use nervix_connector_syslog::{SyslogSink, SyslogSinkConfig};
use nervix_connector_zeromq::{ZeroMqSink, ZeroMqSinkConfig};
use nervix_models::EmitterBatchPolicy;

use super::{
    emitter_http_requests::{HttpRequestBody, PreparedRequestSink},
    pooled_sink_clients::PooledSinkClient,
    *,
};

/// The composition root of the sink side, and the only place that names every sink crate.
///
/// Each variant of an emitter's sink plan maps to its crate's constructor, and the connector that
/// constructor opens is paired with what the host prepares its input with: a record sink with the
/// codec its records are encoded by, a row sink or a row request sink with the projection that maps
/// its columns, and an HTTP sink with the body its requests carry.
pub(super) struct EmitterSinkStarter;

impl EmitterSinkStarter {
    /// Rejects, while the emitter is built, a client configuration its connector can already tell
    /// will never open.
    ///
    /// Only Syslog reads everything it needs from its configuration alone, so it is the only sink
    /// checked before its task starts; every other sink reports its client when it opens.
    pub(super) fn check_client_config(plan: &EmitterStartPlan) -> EmitterRuntimeResult<()> {
        match &plan.sink {
            EmitterSinkPlan::Syslog(sink) => {
                SyslogSink::check_client_config(&sink.client.config.entries)
                    .change_context(EmitterRuntimeError::InvalidSinkConfig)
            }
            EmitterSinkPlan::Client(_)
            | EmitterSinkPlan::Http(_)
            | EmitterSinkPlan::Kafka(_)
            | EmitterSinkPlan::Pulsar(_)
            | EmitterSinkPlan::RabbitMq(_)
            | EmitterSinkPlan::Redis(_)
            | EmitterSinkPlan::Mqtt(_)
            | EmitterSinkPlan::Nats(_)
            | EmitterSinkPlan::ZeroMq(_)
            | EmitterSinkPlan::Sqs(_)
            | EmitterSinkPlan::Sentry(_)
            | EmitterSinkPlan::Otel(_)
            | EmitterSinkPlan::ClickHouse(_)
            | EmitterSinkPlan::Postgres(_)
            | EmitterSinkPlan::MySql(_)
            | EmitterSinkPlan::MongoDb(_)
            | EmitterSinkPlan::Iceberg(_) => Ok(()),
        }
    }

    /// Opens the connector `plan` names.
    pub(super) async fn start(
        plan: &EmitterStartPlan,
        context: &EmitterSinkContext,
        input_schema: &CompiledSchema,
        output_schema: &Arc<CompiledSchema>,
        codec: Option<&Arc<CompiledCodec>>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>> {
        let label = plan.sink.label();
        let batch = plan.sink.batch();
        let sink: Box<dyn EmitterSink> = match &plan.sink {
            EmitterSinkPlan::Client(sink) => Box::new(nervix_primitives::expect_lint!(
                nervix::lifecycle_call,
                "sink initialization publishes one concrete client-emitter endpoint lifetime \
                 before processing records",
                ClientEmitterSink::new(context, sink, output_schema.clone(), plan.retry_policy,)
            )),
            EmitterSinkPlan::Http(sink) => Self::http_request(
                codec,
                HttpSink::new(
                    HttpSinkConfig {
                        config: sink.client.config.entries.clone(),
                        dns: context.dns()?,
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Kafka(sink) => Self::record(
                label,
                codec,
                batch,
                KafkaSink::new(
                    KafkaSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Pulsar(sink) => Self::record(
                label,
                codec,
                batch,
                PulsarSink::new(
                    PulsarSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::RabbitMq(sink) => Self::record(
                label,
                codec,
                batch,
                RabbitMqSink::new(
                    RabbitMqSinkConfig {
                        config: sink.client.config.entries.clone(),
                        dns: context.dns()?,
                        queue: sink.queue.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Redis(sink) => {
                let pool = context.lease_redis_pool(sink).await?;
                Self::record(
                    label,
                    codec,
                    batch,
                    RedisSink::new(
                        RedisSinkConfig {
                            pool,
                            channel: sink.channel.clone(),
                        },
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::Mqtt(sink) => Self::record(
                label,
                codec,
                batch,
                MqttSink::new(
                    MqttSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                        retry_policy: plan.retry_policy,
                        default_client_id: format!(
                            "{}-{}",
                            context.domain.as_str(),
                            context.emitter.as_str()
                        ),
                        dns: context.dns()?,
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Nats(sink) => Self::record(
                label,
                codec,
                batch,
                NatsSink::new(
                    NatsSinkConfig {
                        config: sink.client.config.entries.clone(),
                        subject: sink.subject.clone(),
                        mode: sink.mode,
                        retry_policy: plan.retry_policy,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::ZeroMq(sink) => Self::record(
                label,
                codec,
                batch,
                ZeroMqSink::new(
                    ZeroMqSinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Syslog(sink) => Self::record(
                label,
                codec,
                batch,
                SyslogSink::new(
                    SyslogSinkConfig {
                        config: sink.client.config.entries.clone(),
                        dns: context.dns()?,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Sqs(sink) => Self::record(
                label,
                codec,
                batch,
                SqsSink::new(
                    SqsSinkConfig {
                        config: sink.client.config.entries.clone(),
                        queue: sink.queue.clone(),
                        mode: sink.mode,
                        dns: context.dns()?,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Sentry(sink) => Self::record(
                label,
                codec,
                batch,
                SentrySink::new(
                    SentrySinkConfig {
                        config: sink.client.config.entries.clone(),
                        dns: context.dns()?,
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Otel(sink) => {
                // The signal's own values and its attributes are mapped as one program, so the
                // attribute columns follow the signal's own in the batch the host projects.
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "OTEL",
                    namespace: "otel",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let config = OtelSinkConfig {
                    config: sink.client.config.entries.clone(),
                    dns: context.dns()?,
                    signal: sink.signal.clone(),
                    batch: sink.batch,
                    values: sink.values.clone(),
                    attributes: sink.attributes.clone(),
                    resource: sink.resource.clone(),
                    scope: sink.scope.clone(),
                    mapped_schema: projection.mapped_schema().clone(),
                };
                Self::row_request(projection, OtelSink::new(config, context.sink_host()))?
            }
            EmitterSinkPlan::ClickHouse(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "ClickHouse",
                    namespace: "clickhouse",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                Self::row(
                    projection,
                    ClickHouseSink::new(
                        ClickHouseSinkConfig {
                            config: sink.client.config.entries.clone(),
                            table: sink.table.clone(),
                            dns: context.dns()?,
                            batch: sink.batch,
                        },
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::Postgres(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "Postgres",
                    namespace: "postgres",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let connections =
                    PooledSinkClient::lease(context, &sink.client, sink.pooled_client())
                        .await
                        .map_err(emitter_init_error)?;
                Self::row(
                    projection,
                    PostgresSink::new(
                        PostgresSinkConfig {
                            table: sink.table.clone(),
                            conflict_action: sink.conflict_action.clone(),
                            batch: sink.batch,
                        },
                        Box::new(connections),
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::MySql(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "MySQL",
                    namespace: "mysql",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let connections =
                    PooledSinkClient::lease(context, &sink.client, sink.pooled_client())
                        .await
                        .map_err(emitter_init_error)?;
                Self::row(
                    projection,
                    MySqlSink::new(
                        MySqlSinkConfig {
                            table: sink.table.clone(),
                            conflict_action: sink.conflict_action,
                            batch: sink.batch,
                        },
                        Box::new(connections),
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::MongoDb(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "MongoDB",
                    namespace: "mongodb",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let client = PooledSinkClient::lease(context, &sink.client, sink.pooled_client())
                    .await
                    .map_err(emitter_init_error)?;
                Self::row(
                    projection,
                    MongoDbSink::new(
                        MongoDbSinkConfig {
                            config: sink.client.config.entries.clone(),
                            collection: sink.collection.clone(),
                            conflict_action: sink.conflict_action.clone(),
                            batch: sink.batch,
                        },
                        Box::new(client),
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::Iceberg(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "Iceberg",
                    namespace: "iceberg",
                    emitter: &context.emitter,
                    mapping: &sink.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let mapped_schema = projection.mapped_schema().clone();
                let opened = IcebergSink::new(
                    IcebergSinkConfig {
                        backend: sink.backend,
                        dns: context.dns()?,
                        storage_config: sink.storage.config.entries.clone(),
                        catalog_name: sink.catalog.name.as_str().to_string(),
                        catalog_config: sink.catalog.config.entries.clone(),
                        namespace: context.domain.as_str().to_string(),
                        table: sink.table.clone(),
                        location: sink.location.clone(),
                        mapped_schema,
                        commit: sink.commit,
                        writer: context.emitter.as_str().to_string(),
                    },
                    context.sink_host(),
                )
                .await;
                Self::row(projection, opened)?
            }
        };
        Ok(sink)
    }

    /// Pairs a record sink with the codec the host encodes its records with, and with the `BATCH`
    /// clause whose payloads it publishes when the emitter declares one.
    ///
    /// The registry requires `ENCODE USING` on exactly the sinks that publish encoded records, so a
    /// record sink always receives its codec here.
    fn record<T>(
        label: &str,
        codec: Option<&Arc<CompiledCodec>>,
        batch: Option<EmitterBatchPolicy>,
        started: SinkStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>>
    where
        T: RecordSink + 'static,
    {
        let sink = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let Some(codec) = codec else {
            return Err(Report::new(EmitterRuntimeError::InvalidSinkConfig)
                .attach_printable(format!("{label} emitter requires an encoding codec")));
        };
        let sink: Box<dyn EmitterSink> = Box::new(EncodedRecordSink {
            sink: Box::new(sink),
            codec: codec.clone(),
            batch,
        });
        Ok(sink)
    }

    /// Pairs an HTTP sink with the body the host prepares each of its requests with: the bytes of
    /// the codec an emitter encodes its records with, or no content for an emitter declared
    /// `WITHOUT BODY`, which names no codec.
    fn http_request<T>(
        codec: Option<&Arc<CompiledCodec>>,
        started: SinkStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>>
    where
        T: HttpRequestSink + 'static,
    {
        let sink = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let body = match codec {
            Some(codec) => HttpRequestBody::Encoded(codec.clone()),
            None => HttpRequestBody::Absent,
        };
        let sink: Box<dyn EmitterSink> = Box::new(PreparedRequestSink::new(Box::new(sink), body));
        Ok(sink)
    }

    /// Pairs a row sink with the projection whose mapped columns it writes.
    fn row<T>(
        projection: MappedValuesProjection,
        started: SinkStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>>
    where
        T: RowSink + 'static,
    {
        let sink = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let sink: Box<dyn EmitterSink> = Box::new(MappedRowSink::new(Box::new(sink), projection));
        Ok(sink)
    }

    /// Pairs a row request sink with the projection whose mapped columns it prepares requests from.
    fn row_request<T>(
        projection: MappedValuesProjection,
        started: SinkStartResult<T>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>>
    where
        T: RowRequestSink + 'static,
    {
        let sink = started.change_context(EmitterRuntimeError::InitializeSink)?;
        let sink: Box<dyn EmitterSink> =
            Box::new(MappedRequestSink::new(Box::new(sink), projection));
        Ok(sink)
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
mod tests {
    use nervix_connector::{ParsedRetryPolicy, ResolvedClientConfig, SinkStartError};
    use nervix_models::ClientConfigEntry;

    use super::*;
    use crate::runtime::test_fixtures::named;

    fn plan(sink: EmitterSinkPlan) -> EmitterStartPlan {
        EmitterStartPlan {
            retry_policy: ParsedRetryPolicy {
                backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
            },
            sink,
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
        let valid = plan(EmitterSinkPlan::Syslog(SyslogSinkPlan {
            client: client(&[("protocol", "udp"), ("addr", "127.0.0.1:5514")]),
            batch: None,
        }));
        assert!(EmitterSinkStarter::check_client_config(&valid).is_ok());

        let unopenable = plan(EmitterSinkPlan::Syslog(SyslogSinkPlan {
            client: client(&[("protocol", "tcp"), ("addr", "missing-port")]),
            batch: None,
        }));
        let Err(error) = EmitterSinkStarter::check_client_config(&unopenable) else {
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

        let unchecked = plan(EmitterSinkPlan::ZeroMq(ZeroMqSinkPlan {
            client: client(&[]),
            batch: None,
        }));
        assert!(EmitterSinkStarter::check_client_config(&unchecked).is_ok());
    }

    /// A connector decides why its own configuration is unusable, so the host states only that the
    /// sink could not be initialized and reports the connector's message as the reason it is
    /// unavailable.
    #[test]
    fn a_connector_start_failure_leaves_the_sink_unavailable_with_its_own_message() {
        let started = EmitterSinkStarter::record::<NatsSink>(
            "nats",
            None,
            None,
            Err(
                Report::new(SinkStartError::InvalidConfiguration { sink: "NATS" })
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
