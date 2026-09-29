//! The source composition root, and the one start path every ingestor takes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Starting an ingestor: preparing its quiescence, refusing a second start, compiling
//!   its dependencies, and mapping its input plan to the connector that runs a transport or to the
//!   client source the node hosts, which is the only place an input plan's kind selects anything.
//! - **Depends on.** Ingestor start plans, the host source launcher, every source connector, and
//!   the endpoint and client sources the server keeps.
//! - **Must not know.** NSPL parsing, registry validation, or placement computation.

use super::*;

pub(in crate::runtime) mod endpoint;
pub(in crate::runtime) mod http;
pub(in crate::runtime) mod kafka;
pub(in crate::runtime) mod mqtt;
pub(in crate::runtime) mod nats;
pub(in crate::runtime) mod prometheus;
pub(in crate::runtime) mod pulsar;
pub(in crate::runtime) mod rabbitmq;
pub(in crate::runtime) mod redis_pubsub;
mod source;
pub(in crate::runtime) mod sqs;
pub(in crate::runtime) mod syslog;
pub(in crate::runtime) mod websockets;
pub(in crate::runtime) mod zeromq;

impl Runtime {
    /// Starts one ingestor. Every source takes this path.
    ///
    /// Everything that can fail comes first: the ingestor's dependencies compile and its source
    /// plan composes into opened connector instances. Only then does the host register the source
    /// and start its tasks, so an ingestor that cannot start leaves nothing running behind it.
    pub(in crate::runtime) async fn start_ingestor(
        &self,
        plan: &IngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let IngestorStartPlan { ingestor, input } = plan;
        let quiesce = self.prepare_ingestor_quiescence(&ingestor.domain, ingestor);
        if self.inner.ingestors.contains_key(&ingestor.runtime_key()) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: ingestor.domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let BoundIngestor {
            input,
            dependencies,
        } = self.ingestor_dependencies(ingestor, input).await?;
        let (codec, source) = match input {
            BoundIngestorInput::Transport { codec, source } => (codec, source),
            BoundIngestorInput::Client { plan, generation } => {
                self.host_client_source(ingestor, &plan, generation, quiesce, dependencies);
                return Ok(());
            }
        };
        let source = match source {
            SourceStartPlan::Http(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Kafka(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Pulsar(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Mqtt(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Nats(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::RabbitMq(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::RedisPubSub(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Prometheus(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::ZeroMq(plan) => plan.compose(ingestor).await?,
            SourceStartPlan::Sqs(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Endpoint(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Websockets(plan) => plan.compose(self, ingestor).await?,
            SourceStartPlan::Syslog(plan) => plan.compose(self, ingestor).await?,
        };
        self.host_source(ingestor, quiesce, dependencies, codec, source);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateClientSqs, RabbitMqIngestMode, SqsIngestMode};

    use super::*;

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).assured("the fixture name is valid")
    }

    #[tokio::test]
    async fn sources_that_resolve_names_report_missing_node_dns_as_start_failure() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let cadence: nervix_models::DomainClockPeriod = "1s"
            .parse()
            .assured("the fixture cadence is a positive duration");
        for (source, client_model) in [
            (
                IngestSource::Http {
                    client: client_name.clone(),
                    every: cadence,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientHttp(CreateClientHttp {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Prometheus {
                    client: client_name.clone(),
                    query: "up".to_string(),
                    every: cadence,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientPrometheus(CreateClientPrometheus {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::RabbitMq {
                    client: client_name.clone(),
                    queue: named("events"),
                    instances: NonZeroU64::MIN,
                    mode: RabbitMqIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientRabbitMq(CreateClientRabbitMq {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Sqs {
                    client: client_name.clone(),
                    queue: named("events"),
                    instances: NonZeroU64::MIN,
                    mode: SqsIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: nervix_models::RetryPolicy {
                            backoff: "100ms".to_string(),
                            max_backoff: "1s".to_string(),
                        },
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientSqs(CreateClientSqs {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
        ] {
            let ingestor = CreateIngestor {
                name: named("source"),
                output_routes: with_inherit_all(nervix_models::ProcessorOutputs::single(named(
                    "events",
                )))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
                input: nervix_models::IngestorInput::Transport(
                    nervix_models::TransportIngestorInput {
                        source,
                        codec: named("json"),
                    },
                ),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            };
            let plans = planned_entrypoints_for_test(
                &domain,
                vec![
                    Model::Schema(nervix_models::CreateSchema {
                        name: named("payload"),
                        fields: vec![nervix_models::SchemaField {
                            name: named("value"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::WireJsonSchema(nervix_models::CreateJsonWireSchema {
                        name: named("payload_wire"),
                        strictness: Default::default(),
                        fields: vec![nervix_models::WireSchemaField {
                            name: named("value"),
                            ty: nervix_models::JsonType::String,
                            optional: false,
                        }],
                    }),
                    Model::Codec(nervix_models::CreateCodec {
                        name: named("json"),
                        wire_format: nervix_models::CodecWireFormat::Json {
                            wire_schema: named("payload_wire"),
                        },
                        schema: named("payload"),
                        encoding_rules: Vec::new(),
                    }),
                    Model::Relay(CreateRelay {
                        name: named("events"),
                        schema: named("payload"),
                        buffer: nonzero_ext::nonzero!(2usize),
                        branching: nervix_models::RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                    client_model,
                    Model::Ingestor(ingestor),
                ],
            );
            let plan = plans
                .ingestor(&named("source"))
                .assured("the fixture schedules the ingestor named source");
            let IngestorInputPlan::Transport(transport) = &plan.input else {
                panic!("the fixture ingestor reads a transport");
            };
            let result = match transport.source.clone() {
                SourceStartPlan::Http(source) => source.compose(&runtime, &plan.ingestor).await,
                SourceStartPlan::Prometheus(source) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                SourceStartPlan::RabbitMq(source) => source.compose(&runtime, &plan.ingestor).await,
                SourceStartPlan::Sqs(source) => source.compose(&runtime, &plan.ingestor).await,
                _ => panic!("the fixture only includes sources that resolve names"),
            };
            let Err(RuntimeError::StartIngestor {
                domain,
                ingestor,
                reason,
            }) = result
            else {
                panic!("a source that resolves names must fail without node DNS before it opens");
            };
            assert_eq!(domain, "sales");
            assert_eq!(ingestor, "source");
            assert_eq!(reason, "the node DNS resolver is not installed");
        }
    }
}
