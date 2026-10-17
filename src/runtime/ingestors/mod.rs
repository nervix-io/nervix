//! The source composition root, and the one start path every ingestor takes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Starting an ingestor: preparing its quiescence, refusing a second start, compiling
//!   its dependencies, and mapping its input plan to the connector that runs a transport or to the
//!   client source the node hosts, which is the only place an input plan's kind selects anything.
//! - **Depends on.** Ingestor start plans, the host source launcher, every source connector, and
//!   the endpoint and client sources the server keeps.
//! - **Must not know.** Plexus parsing, registry validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "source installation and intake withdrawal bind one exact connector lifetime"
    )
)]

use super::{ingestor_start::RetiringIngestorInput, *};

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

/// Why this node could not start an ingestor.
///
/// Every failure names the ingestor, or the domain and codec it binds. A failure to initialize the
/// ingestor's source is [`IngestorStartError::Initialize`], with the cause beneath it: a
/// [`SourceStartError`], or the report of the client configuration, connector plan, source
/// instance or domain cadence that failed.
#[derive(Debug, Error)]
pub(crate) enum IngestorStartError {
    #[error("ingestor '{ingestor}' in domain '{domain}' is already running")]
    AlreadyRunning {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error(
        "the execution of domain '{domain}' is not installed while starting ingestor '{ingestor}'"
    )]
    DomainExecutionUnavailable {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("codec '{codec}' in domain '{domain}' is not instantiated")]
    CodecNotInstantiated {
        domain: DomainName,
        codec: CodecName,
    },
    /// The ingestor's programs or routes did not bind; the binding failure is the cause beneath.
    #[error("failed to bind an ingestor or reingestor of domain '{domain}'")]
    Bind { domain: DomainName },
    #[error("failed to initialize ingestor '{ingestor}' in domain '{domain}'")]
    Initialize {
        domain: DomainName,
        ingestor: IngestorName,
    },
}

/// Why an ingestor's client configuration could not resolve or its source could not be composed
/// or opened on this node. Startup wraps it in [`IngestorStartError::Initialize`]; retirement
/// wraps it in [`RuntimeError::RetireIngestorSource`].
#[derive(Debug, Error)]
pub(crate) enum SourceStartError {
    #[error("failed to resolve the source's client configuration")]
    ClientConfiguration,
    #[error("the node DNS resolver is not installed")]
    NodeDnsUnavailable,
    #[error("missing signaling protocol '{protocol}'")]
    SignalingProtocolMissing { protocol: SignalingProtocolName },
    #[error("endpoint '{endpoint}' is not instantiated")]
    EndpointNotInstantiated { endpoint: EndpointName },
    #[error("Kafka DOMAIN offsets are not authoritative on this node")]
    KafkaDomainOffsetsNotAuthoritative,
    #[error("invalid {setting} '{value}'")]
    InvalidDuration {
        setting: DeliverySetting,
        value: String,
        #[source]
        source: nervix_models::DurationTextError,
    },
}

/// A duration an ingestor's delivery mode declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum DeliverySetting {
    #[strum(serialize = "ack timeout")]
    AckTimeout,
    #[strum(serialize = "batch timeout")]
    BatchTimeout,
    #[strum(serialize = "retry backoff")]
    RetryBackoff,
    #[strum(serialize = "retry max backoff")]
    RetryMaxBackoff,
}

impl Runtime {
    /// Applies external retirement after the previous execution stopped. Cleanup is best-effort;
    /// a replacement prepares its own changed sessions before its new input can become ready.
    pub(super) async fn retire_ingestor_input(
        &self,
        retirement: RetiringIngestorInput,
    ) -> error_stack::Result<(), RuntimeError> {
        let IngestorInputPlan::Transport(input) = &retirement.previous.input else {
            return Ok(());
        };
        let SourceStartPlan::Mqtt(source) = &input.source else {
            return Ok(());
        };
        let replacement = match retirement.replacement.as_deref() {
            Some(IngestorStartPlan {
                input: IngestorInputPlan::Transport(input),
                ..
            }) => {
                if let SourceStartPlan::Mqtt(source) = &input.source {
                    Some(source)
                } else {
                    None
                }
            }
            Some(_) | None => None,
        };
        source
            .retire_before(self, &retirement.previous.ingestor, replacement)
            .await
    }

    /// Starts one ingestor. Every source takes this path.
    ///
    /// Everything that can fail comes first: the ingestor's dependencies compile and its source
    /// plan composes into opened connector instances. Only then does the host register the source
    /// and start its tasks, so an ingestor that cannot start leaves nothing running behind it.
    pub(in crate::runtime) async fn start_ingestor(
        &self,
        plan: &IngestorStartPlan,
    ) -> error_stack::Result<(), IngestorStartError> {
        let IngestorStartPlan { ingestor, input } = plan;
        let quiesce = self.prepare_ingestor_quiescence(&ingestor.domain, ingestor);
        if self.inner.ingestors.contains_key(&ingestor.runtime_key()) {
            return Err(Report::new(IngestorStartError::AlreadyRunning {
                domain: ingestor.domain.clone(),
                ingestor: ingestor.name.clone(),
            }));
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
            SourceStartPlan::ZeroMq(plan) => plan.compose(self, ingestor).await?,
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        ClientPoolBounds, CreateClientMqtt, CreateClientRedis, CreateClientSqs, CreateClientZeroMq,
        HttpRequestSource, HttpSource, MqttIngestMode, MqttQos, MqttSession, MqttSource,
        MqttTopicSource, PrometheusQuerySource, PrometheusSource, RabbitMqIngestMode,
        RabbitMqQueueSource, RabbitMqSource, RedisPubSubIngestMode, RedisPubSubSource, RedisSource,
        SqsIngestMode, SqsQueueSource, SqsSource, ZeroMqIngestMode, ZeroMqPullSource, ZeroMqSource,
    };

    use super::*;
    use crate::runtime::resources::RuntimeResourceError;

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).assured("the fixture name is valid")
    }

    /// The entrypoint plans of a domain whose one ingestor, `source`, reads `source` through
    /// `client_model` with a JSON codec into the relay `events`.
    fn planned_source_ingestor(
        domain: &DomainName,
        source: IngestSource,
        client_model: Model,
    ) -> EntrypointPlans {
        let ingestor = CreateIngestor {
            name: named("source"),
            output_routes: with_inherit_all(nervix_models::ProcessorOutputs::single(named(
                "events",
            )))
            .with_flush_policy(FlushPolicy::Immediate)
            .with_branch(OutputBranch::Unbranched),
            input: nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
                source,
                codec: named("json"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };
        planned_entrypoints_for_test(
            domain,
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
        )
    }

    #[nervix_primitives::test]
    async fn an_ingestor_without_its_domain_execution_names_the_missing_execution() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let plans = planned_source_ingestor(
            &domain,
            IngestSource::Http(HttpSource::Request(HttpRequestSource {
                client: client_name.clone(),
                every: "1s"
                    .parse()
                    .assured("the fixture cadence is a positive duration"),
                mode: nervix_models::HttpRequestIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            Model::ClientHttp(CreateClientHttp {
                name: client_name,
                mount: None,
                config: Vec::new(),
            }),
        );
        let plan = plans
            .ingestor(&named("source"))
            .assured("the fixture schedules the ingestor named source");

        let report = runtime
            .start_ingestor(plan)
            .await
            .expect_err("an ingestor binds its dependencies from its domain's execution");

        assert!(
            matches!(
                report.current_context(),
                IngestorStartError::DomainExecutionUnavailable { domain: failed, ingestor }
                    if *failed == domain && ingestor.as_str() == "source"
            ),
            "{report:?}"
        );
        let message =
            "the execution of domain 'sales' is not installed while starting ingestor 'source'";
        assert_eq!(format!("{report:#}"), message);
    }

    #[test]
    fn ingestor_start_failures_keep_their_diagnostic_text() {
        let domain: DomainName = named("sales");
        let ingestor: IngestorName = named("source");
        let initialize = || IngestorStartError::Initialize {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
        };
        let cases = [
            (
                Report::new(IngestorStartError::AlreadyRunning {
                    domain: domain.clone(),
                    ingestor: ingestor.clone(),
                }),
                "ingestor 'source' in domain 'sales' is already running",
            ),
            (
                Report::new(IngestorStartError::CodecNotInstantiated {
                    domain: domain.clone(),
                    codec: named("json"),
                }),
                "codec 'json' in domain 'sales' is not instantiated",
            ),
            (
                Report::new(IngestorStartError::Bind {
                    domain: domain.clone(),
                }),
                "failed to bind an ingestor or reingestor of domain 'sales'",
            ),
            (
                Report::new(SourceStartError::SignalingProtocolMissing {
                    protocol: named("handshake"),
                })
                .change_context(initialize()),
                "failed to initialize ingestor 'source' in domain 'sales': missing signaling \
                 protocol 'handshake'",
            ),
            (
                Report::new(SourceStartError::EndpointNotInstantiated {
                    endpoint: named("events_endpoint"),
                })
                .change_context(initialize()),
                "failed to initialize ingestor 'source' in domain 'sales': endpoint \
                 'events_endpoint' is not instantiated",
            ),
            (
                Report::new(SourceStartError::KafkaDomainOffsetsNotAuthoritative)
                    .change_context(initialize()),
                "failed to initialize ingestor 'source' in domain 'sales': Kafka DOMAIN offsets \
                 are not authoritative on this node",
            ),
        ];
        for (report, message) in cases {
            assert_eq!(format!("{report:#}"), message);
        }
    }

    #[nervix_primitives::test]
    async fn sources_that_resolve_names_report_missing_node_dns_as_start_failure() {
        let runtime = Runtime::default();
        let domain: DomainName = named("sales");
        let client_name: ClientName = named("upstream");
        let cadence: nervix_models::DomainClockPeriod = "1s"
            .parse()
            .assured("the fixture cadence is a positive duration");
        for (source, client_model) in [
            (
                IngestSource::Http(HttpSource::Request(HttpRequestSource {
                    client: client_name.clone(),
                    every: cadence,
                    mode: nervix_models::HttpRequestIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientHttp(CreateClientHttp {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Prometheus(PrometheusSource::Query(PrometheusQuerySource {
                    client: client_name.clone(),
                    query: "up".to_string(),
                    every: cadence,
                    mode: nervix_models::PrometheusIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                Model::ClientPrometheus(CreateClientPrometheus {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::RabbitMq(RabbitMqSource::Queue(RabbitMqQueueSource {
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
                })),
                Model::ClientRabbitMq(CreateClientRabbitMq {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Sqs(SqsSource::Queue(SqsQueueSource {
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
                })),
                Model::ClientSqs(CreateClientSqs {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Redis(RedisSource::PubSub(RedisPubSubSource {
                    client: client_name.clone(),
                    channel: named("events"),
                    mode: RedisPubSubIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Drop,
                })),
                Model::ClientRedis(CreateClientRedis {
                    name: client_name.clone(),
                    pool: ClientPoolBounds::new(0, nonzero_ext::nonzero!(1u32))
                        .assured("zero is below one"),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
            (
                IngestSource::Mqtt(MqttSource::Topic(MqttTopicSource {
                    client: client_name.clone(),
                    topic: nervix_models::MqttTopicFilter::parse("events")
                        .expect("valid topic filter"),
                    instances: NonZeroU64::MIN,
                    mode: MqttIngestMode::NoAck {
                        session: MqttSession::Clean,
                        qos: MqttQos::AtMostOnce,
                    },
                    quiesce: IngestQuiesceMode::Drop,
                })),
                Model::ClientMqtt(CreateClientMqtt {
                    name: client_name.clone(),
                    mount: None,
                    config: Vec::new(),
                }),
            ),
        ] {
            let plans = planned_source_ingestor(&domain, source, client_model);
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
                SourceStartPlan::RedisPubSub(source) => {
                    source.compose(&runtime, &plan.ingestor).await
                }
                SourceStartPlan::Mqtt(source) => source.compose(&runtime, &plan.ingestor).await,
                SourceStartPlan::Sqs(source) => source.compose(&runtime, &plan.ingestor).await,
                _ => panic!("the fixture only includes sources that resolve names"),
            };
            let Err(report) = result else {
                panic!("a source that resolves names must fail without node DNS before it opens");
            };
            let IngestorStartError::Initialize { domain, ingestor } = report.current_context()
            else {
                panic!("the failure must name the ingestor that could not start: {report:?}");
            };
            assert_eq!(domain.as_str(), "sales");
            assert_eq!(ingestor.as_str(), "source");
            assert!(
                matches!(
                    report.downcast_ref::<SourceStartError>(),
                    Some(SourceStartError::NodeDnsUnavailable)
                ),
                "the missing resolver must be the typed cause: {report:?}"
            );
            assert_eq!(
                format!("{report:#}"),
                "failed to initialize ingestor 'source' in domain 'sales': the node DNS resolver \
                 is not installed"
            );
        }
    }

    /// The ZeroMQ client `upstream`, reading `addr` with the optional resource mount.
    fn zeromq_client(mount: Option<ClientResourceMount>, addr: &str) -> CreateClientZeroMq {
        CreateClientZeroMq {
            name: named("upstream"),
            mount,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
        }
    }

    /// Composes the ZeroMQ ingestor `source` of domain `sales`, which reads `client`.
    async fn compose_zeromq_source(
        runtime: &Runtime,
        client: CreateClientZeroMq,
    ) -> error_stack::Result<source::SourceStart, IngestorStartError> {
        let domain: DomainName = named("sales");
        let pull = IngestSource::ZeroMq(ZeroMqSource::Pull(ZeroMqPullSource {
            client: client.name.clone(),
            mode: ZeroMqIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        }));
        let plans = planned_source_ingestor(&domain, pull, Model::ClientZeroMq(client));
        let plan = plans
            .ingestor(&named("source"))
            .assured("the fixture schedules the ingestor named source");
        let IngestorInputPlan::Transport(transport) = &plan.input else {
            panic!("the fixture ingestor reads a transport");
        };
        let SourceStartPlan::ZeroMq(zeromq) = transport.source.clone() else {
            panic!("the fixture ingestor reads a ZeroMQ client");
        };
        zeromq.compose(runtime, &plan.ingestor).await
    }

    #[nervix_primitives::test]
    async fn a_zeromq_source_resolves_its_client_configuration_before_it_opens() {
        let runtime = Runtime::default();
        let feed = ClientResourceMount {
            resource: named("feed"),
            version: 1,
        };
        let cases = [
            (
                zeromq_client(None, "ipc://{{ feed }}/ingest.sock"),
                "client configuration entry 'addr' has an invalid template",
            ),
            (
                zeromq_client(Some(feed), "ipc://{{ feed }}/ingest.sock"),
                "client resource 'feed' in domain 'sales' requires a resource store",
            ),
        ];
        for (client, cause) in cases {
            let Err(report) = compose_zeromq_source(&runtime, client).await else {
                panic!("a ZeroMQ source must resolve its client configuration before it opens");
            };
            assert!(
                matches!(
                    report.current_context(),
                    IngestorStartError::Initialize { domain, ingestor }
                        if domain.as_str() == "sales" && ingestor.as_str() == "source"
                ),
                "the failure must name the ingestor that could not start: {report:?}"
            );
            assert!(
                report.contains::<RuntimeResourceError>(),
                "the client configuration failure must be the typed cause: {report:?}"
            );
            assert_eq!(
                format!("{report:#}"),
                format!("failed to initialize ingestor 'source' in domain 'sales': {cause}")
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_zeromq_source_renders_its_address_over_a_mount_it_keeps() {
        let store_root = tempfile::tempdir().expect("resource store tempdir");
        let content_root = tempfile::tempdir().expect("resource content tempdir");
        std::fs::write(content_root.path().join("feed.txt"), "feed")
            .expect("resource file should be written");
        let store = ResourceStore::open(store_root.path(), Executor::default())
            .expect("resource store should open");
        store
            .install_from_directory(
                ResourceId::new(named("sales"), named("feed"), 1),
                content_root.path(),
                named("node-1"),
                Timestamp::from_unix_nanos(0),
            )
            .await
            .expect("resource version should install");
        let runtime = Runtime::default();
        runtime.attach_resource_store(StdArc::new(store));
        let feed = ClientResourceMount {
            resource: named("feed"),
            version: 1,
        };

        let composed = compose_zeromq_source(
            &runtime,
            zeromq_client(Some(feed), "ipc://{{ feed }}/ingest.sock"),
        )
        .await;

        let start = match composed {
            Ok(start) => start,
            Err(report) => panic!("a ZeroMQ source over an installed mount composes: {report:?}"),
        };
        assert_eq!(start.instances.len(), 1);
        assert_eq!(
            start.client_mounts.len(),
            1,
            "the source keeps its client mount for as long as its instance runs"
        );
    }
}
