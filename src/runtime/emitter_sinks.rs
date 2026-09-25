//! The composition root of the emitter host's sink side.
//!
//! Layer: data plane.
//! - **Owns.** Mapping each variant of an emitter's sink plan to its connector crate's constructor,
//!   and pairing the connector it opens with the input the host prepares for its contract. It is
//!   the only place that names every sink crate.
//! - **Depends on.** The emitter start plan, the connector crates and their contract, the host's
//!   values projection and record encoding, and the pooled client leases.
//! - **Must not know.** When the emitter publishes, how it buffers or retries, or how a batch is
//!   encoded or mapped.

use error_stack::ResultExt as _;
use nervix_connector::{RecordSink, RowSink, SinkStartResult};
use nervix_connector_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use nervix_connector_iceberg::{IcebergCommitPolicy, IcebergSink, IcebergSinkConfig};
use nervix_connector_kafka::{KafkaSink, KafkaSinkConfig};
use nervix_connector_mongodb::{MongoDbSink, MongoDbSinkConfig};
use nervix_connector_mqtt::{MqttSink, MqttSinkConfig};
use nervix_connector_mysql::{MySqlSink, MySqlSinkConfig};
use nervix_connector_nats::{NatsSink, NatsSinkConfig};
use nervix_connector_otel::{OtelLiteral, OtelResourceAttribute, OtelSink, OtelSinkConfig};
use nervix_connector_postgres::{PostgresSink, PostgresSinkConfig};
use nervix_connector_pulsar::{PulsarSink, PulsarSinkConfig};
use nervix_connector_rabbitmq::{RabbitMqSink, RabbitMqSinkConfig};
use nervix_connector_redis::{RedisSink, RedisSinkConfig};
use nervix_connector_sentry::{SentrySink, SentrySinkConfig};
use nervix_connector_sqs::{SqsSink, SqsSinkConfig};
use nervix_connector_syslog::{SyslogSink, SyslogSinkConfig};
use nervix_connector_zeromq::{ZeroMqSink, ZeroMqSinkConfig};

use super::{pooled_sink_clients::PooledSinkClient, *};

fn mapped_column_names(mappings: &[ClickHouseValueMapping]) -> Vec<String> {
    mappings
        .iter()
        .map(|mapping| mapping.column.clone())
        .collect()
}

/// The `RESOURCE` attributes an OTEL emitter exports with, which are fixed for its lifetime.
///
/// A resource value describes the emitting service rather than a record, so only a literal or a
/// literal array can supply one.
fn otel_resource_attributes(
    resource: &[OtelValueMapping],
) -> EmitterRuntimeResult<Vec<OtelResourceAttribute>> {
    let mut attributes = Vec::with_capacity(resource.len());
    for mapping in resource {
        let value = otel_literal(&mapping.expression).ok_or_else(|| {
            Report::new(EmitterRuntimeError::InvalidOtelResource {
                attribute: mapping.column.clone(),
            })
        })?;
        attributes.push(OtelResourceAttribute {
            key: mapping.column.clone(),
            value,
        });
    }
    Ok(attributes)
}

/// The literal a `RESOURCE` value carries, or nothing for an expression that reads a record.
fn otel_literal(expression: &nervix_models::Expression) -> Option<OtelLiteral> {
    match expression {
        nervix_models::Expression::Literal(ModelLiteral::I64(value)) => {
            Some(OtelLiteral::I64(*value))
        }
        nervix_models::Expression::Literal(ModelLiteral::F64(value)) => {
            Some(OtelLiteral::F64(value.value()))
        }
        nervix_models::Expression::Literal(ModelLiteral::Bool(value)) => {
            Some(OtelLiteral::Bool(*value))
        }
        nervix_models::Expression::Literal(ModelLiteral::String(value)) => {
            Some(OtelLiteral::String(value.clone()))
        }
        nervix_models::Expression::Literal(ModelLiteral::Null) => Some(OtelLiteral::Null),
        nervix_models::Expression::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(otel_literal(item)?);
            }
            Some(OtelLiteral::Array(values))
        }
        _ => None,
    }
}

impl EmitterSinkContext {
    /// The commit cadence and maximum commit size a sink that publishes on its own commit
    /// boundary was declared with, resolved here so the connector receives typed policy.
    fn parse_commit_policy(
        &self,
        kind: &str,
        commit_each: &str,
        max_commit_size: &str,
    ) -> EmitterRuntimeResult<IcebergCommitPolicy> {
        let interval = Runtime::parse_runtime_node_duration_setting(
            &self.domain,
            kind,
            &self.emitter,
            "commit_each",
            commit_each,
        )
        .map_err(|error| emitter_report(EmitterRuntimeError::InvalidSinkConfig, error))?;
        let max_size = max_commit_size
            .parse::<ubyte::ByteUnit>()
            .map_err(|error| {
                Report::new(EmitterRuntimeError::InvalidSinkConfig)
                    .attach_printable(format!("max_commit_size '{max_commit_size}': {error}"))
            })?
            .as_u64();
        Ok(IcebergCommitPolicy { interval, max_size })
    }
}

/// The composition root of the sink side, and the only place that names every sink crate.
///
/// Each variant of an emitter's sink plan maps to its crate's constructor, and the connector that
/// constructor opens is paired with what the host prepares its input with: a record sink with the
/// codec its records are encoded by, a row sink with the projection that maps its columns.
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
            EmitterSinkPlan::Kafka(_)
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
        codec: Option<&Arc<CompiledCodec>>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterSink>> {
        let label = plan.sink.label();
        let sink = match &plan.sink {
            EmitterSinkPlan::Kafka(sink) => Self::record(
                label,
                codec,
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
                RabbitMqSink::new(
                    RabbitMqSinkConfig {
                        config: sink.client.config.entries.clone(),
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
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Nats(sink) => Self::record(
                label,
                codec,
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
                SyslogSink::new(
                    SyslogSinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Sqs(sink) => Self::record(
                label,
                codec,
                SqsSink::new(
                    SqsSinkConfig {
                        config: sink.client.config.entries.clone(),
                        queue: sink.queue.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            )?,
            EmitterSinkPlan::Sentry(sink) => Self::record(
                label,
                codec,
                SentrySink::new(
                    SentrySinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                ),
            )?,
            EmitterSinkPlan::Otel(sink) => {
                // The signal's own values and its attributes are mapped as one program, so the
                // attribute columns follow the signal's own in the batch the host projects.
                let mut mappings = Vec::with_capacity(
                    sink.values
                        .len()
                        .checked_add(sink.attributes.len())
                        .assured("an emitter maps fewer columns than usize can count"),
                );
                mappings.extend_from_slice(&sink.values);
                mappings.extend_from_slice(&sink.attributes);
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "OTEL",
                    namespace: "otel",
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &mappings,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: None,
                })?;
                let resource = otel_resource_attributes(&sink.resource)?;
                let config = OtelSinkConfig {
                    config: sink.client.config.entries.clone(),
                    signal: sink.signal.clone(),
                    values: mapped_column_names(&sink.values),
                    attributes: mapped_column_names(&sink.attributes),
                    resource,
                    scope: sink.scope.clone(),
                    mapped_schema: projection.mapped_schema().clone(),
                };
                Self::row(projection, OtelSink::new(config, context.sink_host()))?
            }
            EmitterSinkPlan::ClickHouse(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "ClickHouse",
                    namespace: "clickhouse",
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: Some(sink.batch.max_messages),
                })?;
                Self::row(
                    projection,
                    ClickHouseSink::new(
                        ClickHouseSinkConfig {
                            config: sink.client.config.entries.clone(),
                            table: sink.table.clone(),
                        },
                        context.sink_host(),
                    ),
                )?
            }
            EmitterSinkPlan::Postgres(sink) => {
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: "Postgres",
                    namespace: "postgres",
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: Some(sink.batch.max_messages),
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
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: Some(sink.batch.max_messages),
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
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: Some(sink.batch.max_messages),
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
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    // One commit reads every staged file back at once, so a staged write carries
                    // the whole batch the host released to it.
                    max_batch: None,
                })?;
                let commit = context.parse_commit_policy(
                    "iceberg emitter",
                    &sink.commit_each,
                    &sink.max_commit_size,
                )?;
                let mapped_schema = projection.mapped_schema().clone();
                let opened = IcebergSink::new(
                    IcebergSinkConfig {
                        backend: sink.backend,
                        storage_config: sink.storage.config.entries.clone(),
                        catalog_name: sink.catalog.name.as_str().to_string(),
                        catalog_config: sink.catalog.config.entries.clone(),
                        namespace: context.domain.as_str().to_string(),
                        table: sink.table.clone(),
                        location: sink.location.clone(),
                        mapped_schema,
                        commit,
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

    /// Pairs a record sink with the codec the host encodes its records with.
    ///
    /// The registry requires `ENCODE USING` on exactly the sinks that publish encoded records, so a
    /// record sink always receives its codec here.
    fn record<T>(
        label: &str,
        codec: Option<&Arc<CompiledCodec>>,
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
        });
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

    /// Compiles one row sink's `VALUES` mapping before the sink it feeds is opened.
    ///
    /// A mapping that cannot compile never produces a column, so the emitter reports the failure
    /// the same way it reports a client it could not open and recompiles on its next attempt.
    fn projection(
        init: MappedValuesProjectionInit<'_>,
    ) -> EmitterRuntimeResult<MappedValuesProjection> {
        MappedValuesProjection::compile(init).map_err(emitter_init_error)
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
