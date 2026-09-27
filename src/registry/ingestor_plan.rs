//! Decides the complete specification for starting one scheduled ingestor.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an ingestor and the source it reads into one typed start plan, and
//!   validating its source name, source kind, client, codec and route identities together.
//! - **Depends on.** Validated schedule Models, the entrypoint route planner and vocabulary
//!   values.
//! - **Must not know.** Tokio, locks, shared maps, connector I/O, node-local resources, or task
//!   spawning.

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{
    ClientConfigEntry, ClientName, ClientResourceMount, ClusterNodeName, CodecName,
    ConsumerGroupName, CreateIngestor, DomainName, EndpointName, IngestAcknowledgement,
    IngestQuiesceMode, IngestSource, IngestSourceKind, IngestTimestampSource, IngestorName,
    KafkaIngestMode, KafkaOffsetMode, MessageErrorOperation, Model, ModelName, MqttIngestMode,
    PulsarIngestMode, RabbitMqIngestMode, ScheduledNode, SignalingProtocolName, SqsIngestMode,
};

use super::entrypoint_plan::{
    EntrypointOwner, EntrypointPlanError, EntrypointRouteContext, LoweredFilter, PlannedEntryRoute,
};

/// The source an ingestor declares, asked for its transport class and the quiesce modes it honors.
///
/// Both come from the source vocabulary, which is the one declaration of what a source carries
/// and which quiesce modes it supports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeclaredIngestSource {
    source: IngestSource,
}

impl DeclaredIngestSource {
    /// The transport class of the source, which decides whether its messages carry headers and
    /// which metadata they expose to the ingestor's programs.
    pub(crate) fn transport(&self) -> IngestSourceKind {
        self.source.transport_kind()
    }

    pub(crate) fn quiesce_mode(&self) -> &IngestQuiesceMode {
        self.source.quiesce()
    }

    pub(crate) fn supports_quiesce(&self, mode: &IngestQuiesceMode) -> bool {
        self.source.supports_quiesce(mode)
    }
}

/// The ingestor itself: what it decodes, filters and routes, independent of the source it reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IngestorSpec {
    pub(crate) domain: DomainName,
    pub(crate) name: IngestorName,
    /// Every output route in declared order, which is never empty.
    pub(crate) routes: Vec<PlannedEntryRoute>,
    pub(crate) decode_using_codec: CodecName,
    pub(crate) timestamp_source: Option<IngestTimestampSource>,
    pub(crate) filter_where: Option<LoweredFilter>,
    pub(crate) declared_source: DeclaredIngestSource,
}

impl IngestorSpec {
    /// Whether the ingestor's programs may read the transport headers of its messages.
    pub(crate) fn reads_headers(&self) -> bool {
        self.declared_source.transport().reads_headers()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IngestorClientSpec {
    pub(crate) mount: Option<ClientResourceMount>,
    pub(crate) config: Vec<ClientConfigEntry>,
}

impl IngestorClientSpec {
    /// The client a source reads through, once the client it resolved to is the one its statement
    /// names.
    fn resolved(
        ingestor: &IngestorName,
        expected: &ClientName,
        resolved: &ClientName,
        mount: Option<&ClientResourceMount>,
        config: &[ClientConfigEntry],
    ) -> Result<Self, Report<EntrypointPlanError>> {
        if expected != resolved {
            return Err(Report::new(EntrypointPlanError::SourceIdentityMismatch {
                ingestor: ingestor.clone(),
                expected: ModelName::from(expected),
                resolved: ModelName::from(resolved),
            }));
        }
        Ok(Self {
            mount: mount.cloned(),
            config: config.to_vec(),
        })
    }
}

macro_rules! client_plan {
    ($name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub(crate) struct $name {
            pub(crate) client: IngestorClientSpec,
            $(pub(crate) $field: $type,)*
        }
    };
}

client_plan!(HttpIngestorStartPlan {
    every: nervix_models::DomainClockPeriod,
});

/// Where a Kafka ingestor keeps the offsets it resumes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KafkaOffsetPlan {
    /// The broker keeps them for this consumer group.
    ConsumerGroup(ConsumerGroupName),
    /// The domain keeps them as node-owned state, originated by the scheduled primary.
    Domain(KafkaDomainOffsetPlacement),
}

/// The cluster node that originates a Kafka ingestor's domain offsets. An ingestor planned without
/// a placement, as a graph is before the cluster schedules it, has no originating node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KafkaDomainOffsetPlacement {
    pub(crate) primary_node: Option<ClusterNodeName>,
}

client_plan!(KafkaIngestorStartPlan {
    topic: nervix_models::TopicName,
    offsets: KafkaOffsetPlan,
    instances: NonZeroU64,
    mode: KafkaIngestMode,
});
client_plan!(PulsarIngestorStartPlan {
    topic: nervix_models::TopicName,
    subscription: nervix_models::PulsarSubscriptionName,
    instances: NonZeroU64,
    mode: PulsarIngestMode,
});
client_plan!(MqttIngestorStartPlan {
    topic: String,
    instances: NonZeroU64,
    mode: MqttIngestMode,
});
client_plan!(NatsIngestorStartPlan {
    subject: nervix_models::SubjectName,
    queue_group: nervix_models::QueueGroupName,
    instances: NonZeroU64,
    mode: nervix_models::NatsIngestMode,
});
client_plan!(RabbitMqIngestorStartPlan {
    queue: nervix_models::QueueName,
    instances: NonZeroU64,
    mode: RabbitMqIngestMode,
});
client_plan!(RedisPubSubIngestorStartPlan {
    channel: nervix_models::ChannelName,
    mode: nervix_models::RedisPubSubIngestMode,
});
client_plan!(PrometheusIngestorStartPlan {
    query: String,
    every: nervix_models::DomainClockPeriod,
});
client_plan!(ZeroMqIngestorStartPlan {
    mode: nervix_models::ZeroMqIngestMode,
});
client_plan!(SqsIngestorStartPlan {
    queue: nervix_models::QueueName,
    instances: NonZeroU64,
    mode: SqsIngestMode,
});
client_plan!(WebsocketsIngestorStartPlan {
    mode: nervix_models::WebsocketsIngestMode,
    signaling_protocol: Option<SignalingProtocolName>,
});
client_plan!(SyslogIngestorStartPlan {});

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndpointIngestorStartPlan {
    pub(crate) endpoint: EndpointName,
    pub(crate) mode: nervix_models::EndpointIngestMode,
}

/// Everything the host needs to start one ingestor: the ingestor itself and the plan of the source
/// it reads from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IngestorStartPlan {
    pub(crate) ingestor: IngestorSpec,
    pub(crate) source: SourceStartPlan,
}

/// The plan of one ingestor's source, which the composition root maps to the connector that runs
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceStartPlan {
    Http(HttpIngestorStartPlan),
    Kafka(KafkaIngestorStartPlan),
    Pulsar(PulsarIngestorStartPlan),
    Mqtt(MqttIngestorStartPlan),
    Nats(NatsIngestorStartPlan),
    RabbitMq(RabbitMqIngestorStartPlan),
    RedisPubSub(RedisPubSubIngestorStartPlan),
    Prometheus(PrometheusIngestorStartPlan),
    ZeroMq(ZeroMqIngestorStartPlan),
    Sqs(SqsIngestorStartPlan),
    Endpoint(EndpointIngestorStartPlan),
    Websockets(WebsocketsIngestorStartPlan),
    Syslog(SyslogIngestorStartPlan),
}

impl IngestorStartPlan {
    /// Decides the start plan of one scheduled ingestor from the node its source resolved to.
    pub(in crate::registry) fn decide(
        domain: &DomainName,
        scheduled: &ScheduledNode,
        ingestor: &CreateIngestor,
        source_model: &Model,
        routes: &EntrypointRouteContext<'_>,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let source = SourceStartPlan::decide(ingestor, source_model, scheduled)?;
        let Some(codec) = routes.activation().codecs.get(&ingestor.decode_using_codec) else {
            return Err(Report::new(EntrypointPlanError::MissingCodec {
                ingestor: ingestor.name.clone(),
                codec: ingestor.decode_using_codec.clone(),
            }));
        };
        let identifier = ModelName::from(&ingestor.name);
        let owner = EntrypointOwner::ingestor(&identifier, ingestor);
        let input = codec.schema.arrow_schema();
        let routes = routes.plan_routes(&owner, ingestor.output_routes.outputs(), &input)?;
        let filter_where = match ingestor.filter_where.as_ref() {
            Some(filter) => Some(LoweredFilter::planned(
                &owner,
                filter,
                MessageErrorOperation::FilterWhere,
            )?),
            None => None,
        };
        let ingestor = IngestorSpec {
            domain: domain.clone(),
            name: ingestor.name.clone(),
            routes,
            decode_using_codec: ingestor.decode_using_codec.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            filter_where,
            declared_source: DeclaredIngestSource {
                source: ingestor.source.clone(),
            },
        };
        Ok(Self { ingestor, source })
    }

    /// The acknowledgement the source's delivery mode declares. A source whose statement declares
    /// no delivery mode acknowledges nothing.
    pub(crate) fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match &self.source {
            SourceStartPlan::Kafka(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Pulsar(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Mqtt(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Nats(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::RabbitMq(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::RedisPubSub(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::ZeroMq(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Sqs(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Endpoint(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Websockets(plan) => plan.mode.acknowledgement(),
            SourceStartPlan::Http(_)
            | SourceStartPlan::Prometheus(_)
            | SourceStartPlan::Syslog(_) => IngestAcknowledgement::Unacknowledged,
        }
    }

    /// Whether this ingestor keeps the offsets it resumes from as domain state, which its
    /// placement then replicates.
    pub(crate) fn keeps_domain_offsets(&self) -> bool {
        matches!(
            &self.source,
            SourceStartPlan::Kafka(KafkaIngestorStartPlan {
                offsets: KafkaOffsetPlan::Domain(_),
                ..
            })
        )
    }
}

impl SourceStartPlan {
    fn decide(
        ingestor: &CreateIngestor,
        source_model: &Model,
        scheduled: &ScheduledNode,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let name = &ingestor.name;
        match (&ingestor.source, source_model) {
            (
                IngestSource::Http {
                    client: expected,
                    every,
                    ..
                },
                Model::ClientHttp(resolved),
            ) => Ok(Self::Http(HttpIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                every: *every,
            })),
            (
                IngestSource::Kafka {
                    client: expected,
                    topic,
                    offset_mode,
                    instances,
                    mode,
                    ..
                },
                Model::ClientKafka(resolved),
            ) => {
                let offsets = match offset_mode {
                    KafkaOffsetMode::ConsumerGroup(group) => {
                        KafkaOffsetPlan::ConsumerGroup(group.clone())
                    }
                    KafkaOffsetMode::Domain => {
                        KafkaOffsetPlan::Domain(KafkaDomainOffsetPlacement {
                            primary_node: scheduled.primary_node.clone(),
                        })
                    }
                };
                Ok(Self::Kafka(KafkaIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    topic: topic.clone(),
                    offsets,
                    instances: *instances,
                    mode: mode.clone(),
                }))
            }
            (
                IngestSource::Pulsar {
                    client: expected,
                    topic,
                    subscription,
                    instances,
                    mode,
                    ..
                },
                Model::ClientPulsar(resolved),
            ) => Ok(Self::Pulsar(PulsarIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                topic: topic.clone(),
                subscription: subscription.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::Mqtt {
                    client: expected,
                    topic,
                    instances,
                    mode,
                    ..
                },
                Model::ClientMqtt(resolved),
            ) => Ok(Self::Mqtt(MqttIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                topic: topic.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::Nats {
                    client: expected,
                    subject,
                    queue_group,
                    instances,
                    mode,
                    ..
                },
                Model::ClientNats(resolved),
            ) => Ok(Self::Nats(NatsIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                subject: subject.clone(),
                queue_group: queue_group.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::RabbitMq {
                    client: expected,
                    queue,
                    instances,
                    mode,
                    ..
                },
                Model::ClientRabbitMq(resolved),
            ) => Ok(Self::RabbitMq(RabbitMqIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                queue: queue.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::RedisPubSub {
                    client: expected,
                    channel,
                    mode,
                    ..
                },
                Model::ClientRedis(resolved),
            ) => Ok(Self::RedisPubSub(RedisPubSubIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                channel: channel.clone(),
                mode: mode.clone(),
            })),
            (
                IngestSource::Prometheus {
                    client: expected,
                    query,
                    every,
                    ..
                },
                Model::ClientPrometheus(resolved),
            ) => Ok(Self::Prometheus(PrometheusIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                query: query.clone(),
                every: *every,
            })),
            (
                IngestSource::ZeroMq {
                    client: expected,
                    mode,
                    ..
                },
                Model::ClientZeroMq(resolved),
            ) => Ok(Self::ZeroMq(ZeroMqIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                mode: mode.clone(),
            })),
            (
                IngestSource::Sqs {
                    client: expected,
                    queue,
                    instances,
                    mode,
                    ..
                },
                Model::ClientSqs(resolved),
            ) => Ok(Self::Sqs(SqsIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                queue: queue.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::Endpoint {
                    endpoint: expected,
                    mode,
                    ..
                },
                Model::Endpoint(resolved),
            ) => {
                if expected != &resolved.name {
                    return Err(Report::new(EntrypointPlanError::SourceIdentityMismatch {
                        ingestor: name.clone(),
                        expected: ModelName::from(expected),
                        resolved: ModelName::from(&resolved.name),
                    }));
                }
                Ok(Self::Endpoint(EndpointIngestorStartPlan {
                    endpoint: resolved.name.clone(),
                    mode: mode.clone(),
                }))
            }
            (
                IngestSource::Websockets {
                    client: expected,
                    mode,
                    ..
                },
                Model::ClientWebsockets(resolved),
            ) => Ok(Self::Websockets(WebsocketsIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
                mode: mode.clone(),
                signaling_protocol: resolved.signaling_protocol.clone(),
            })),
            (
                IngestSource::Syslog {
                    client: expected, ..
                },
                Model::ClientSyslog(resolved),
            ) => Ok(Self::Syslog(SyslogIngestorStartPlan {
                client: IngestorClientSpec::resolved(
                    name,
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
            })),
            _ => Err(Report::new(EntrypointPlanError::SourceKindMismatch {
                ingestor: name.clone(),
                resolved: source_model.kind(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        ClientPoolBounds, CreateClientHttp, CreateClientKafka, CreateClientMqtt, CreateClientNats,
        CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
        CreateClientSqs, CreateClientSyslog, CreateClientWebsockets, CreateClientZeroMq,
        CreateEndpoint, EndpointType, FlushPolicy, GeneralErrorPolicy, IngestorName, MqttQos,
        MqttSession, NodeRef, OutputBranch, ProcessorOutputs,
    };
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;
    use crate::registry::{
        EntrypointPlans,
        test_fixtures::{
            codec, named, planned_entrypoints, relay, schema, unlowerable_predicate,
            unplaced_schedule, vhost, wire_schema, with_inherit_all,
        },
    };

    /// Pool bounds for the client fixtures, whose subject is the ingestor start plan rather than
    /// the declared capacity.
    fn pool_bounds() -> ClientPoolBounds {
        ClientPoolBounds::new(1, nonzero!(4u32)).assured("one does not exceed four")
    }

    fn ingestor_model(source: IngestSource) -> Model {
        Model::Ingestor(CreateIngestor {
            name: named("source"),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("events")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            decode_using_codec: named("json"),
            timestamp_source: None,
            source,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        })
    }

    /// The domain one ingestor reads from `source_model` in: its schema, codec, relay and the
    /// VHOST an endpoint source publishes on.
    fn domain_models(source: IngestSource, source_model: Model) -> Vec<Model> {
        vec![
            schema("payload"),
            wire_schema("event_wire"),
            codec("json", "payload"),
            relay("events", "payload"),
            vhost("public", &["events.example.com"]),
            source_model,
            ingestor_model(source),
        ]
    }

    fn start_plan(plans: &EntrypointPlans) -> &IngestorStartPlan {
        plans
            .ingestor(&named::<IngestorName>("source"))
            .assured("the fixture schedules the ingestor named source")
    }

    fn cadence() -> nervix_models::DomainClockPeriod {
        "1s".parse()
            .assured("the fixture cadence is a positive duration")
    }

    fn retry_policy() -> nervix_models::RetryPolicy {
        nervix_models::RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        }
    }

    /// The client every client-backed fixture resolves to.
    fn client() -> IngestorClientSpec {
        IngestorClientSpec {
            mount: None,
            config: Vec::new(),
        }
    }

    /// One source statement, the model it resolves to, and the source plan and header access they
    /// decide.
    struct SourceCase {
        source: IngestSource,
        source_model: Model,
        plan: SourceStartPlan,
        reads_headers: bool,
    }

    fn http_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Http {
                client: named("upstream"),
                every: cadence(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientHttp(CreateClientHttp {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Http(HttpIngestorStartPlan {
                client: client(),
                every: cadence(),
            }),
            reads_headers: true,
        }
    }

    fn kafka_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Kafka {
                client: named("upstream"),
                topic: named("events"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(named("nervix")),
                instances: nonzero!(1u64),
                mode: KafkaIngestMode::NoAckParallel,
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientKafka(CreateClientKafka {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Kafka(KafkaIngestorStartPlan {
                client: client(),
                topic: named("events"),
                offsets: KafkaOffsetPlan::ConsumerGroup(named("nervix")),
                instances: nonzero!(1u64),
                mode: KafkaIngestMode::NoAckParallel,
            }),
            reads_headers: true,
        }
    }

    fn pulsar_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Pulsar {
                client: named("upstream"),
                topic: named("events"),
                subscription: named("nervix"),
                instances: nonzero!(1u64),
                mode: PulsarIngestMode::NoAckParallel,
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientPulsar(CreateClientPulsar {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Pulsar(PulsarIngestorStartPlan {
                client: client(),
                topic: named("events"),
                subscription: named("nervix"),
                instances: nonzero!(1u64),
                mode: PulsarIngestMode::NoAckParallel,
            }),
            reads_headers: true,
        }
    }

    fn mqtt_mode() -> MqttIngestMode {
        MqttIngestMode::NoAckSequential {
            session: MqttSession::Clean,
            qos: MqttQos::AtMostOnce,
        }
    }

    fn mqtt_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Mqtt {
                client: named("upstream"),
                topic: "events/#".to_string(),
                instances: nonzero!(1u64),
                mode: mqtt_mode(),
                quiesce: IngestQuiesceMode::Drop,
            },
            source_model: Model::ClientMqtt(CreateClientMqtt {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Mqtt(MqttIngestorStartPlan {
                client: client(),
                topic: "events/#".to_string(),
                instances: nonzero!(1u64),
                mode: mqtt_mode(),
            }),
            reads_headers: false,
        }
    }

    fn nats_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Nats {
                client: named("upstream"),
                subject: named("events"),
                queue_group: named("nervix"),
                instances: nonzero!(1u64),
                mode: nervix_models::NatsIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Drop,
            },
            source_model: Model::ClientNats(CreateClientNats {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Nats(NatsIngestorStartPlan {
                client: client(),
                subject: named("events"),
                queue_group: named("nervix"),
                instances: nonzero!(1u64),
                mode: nervix_models::NatsIngestMode::NoAckSequential,
            }),
            reads_headers: true,
        }
    }

    fn rabbitmq_mode() -> RabbitMqIngestMode {
        RabbitMqIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: retry_policy(),
        }
    }

    fn rabbitmq_case() -> SourceCase {
        SourceCase {
            source: IngestSource::RabbitMq {
                client: named("upstream"),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: rabbitmq_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientRabbitMq(CreateClientRabbitMq {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::RabbitMq(RabbitMqIngestorStartPlan {
                client: client(),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: rabbitmq_mode(),
            }),
            reads_headers: true,
        }
    }

    fn redis_pubsub_case() -> SourceCase {
        SourceCase {
            source: IngestSource::RedisPubSub {
                client: named("upstream"),
                channel: named("events"),
                mode: nervix_models::RedisPubSubIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Drop,
            },
            source_model: Model::ClientRedis(CreateClientRedis {
                name: named("upstream"),
                pool: pool_bounds(),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::RedisPubSub(RedisPubSubIngestorStartPlan {
                client: client(),
                channel: named("events"),
                mode: nervix_models::RedisPubSubIngestMode::NoAckSequential,
            }),
            reads_headers: false,
        }
    }

    fn prometheus_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Prometheus {
                client: named("upstream"),
                query: "up".to_string(),
                every: cadence(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientPrometheus(CreateClientPrometheus {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Prometheus(PrometheusIngestorStartPlan {
                client: client(),
                query: "up".to_string(),
                every: cadence(),
            }),
            reads_headers: false,
        }
    }

    fn zeromq_case() -> SourceCase {
        SourceCase {
            source: IngestSource::ZeroMq {
                client: named("upstream"),
                mode: nervix_models::ZeroMqIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientZeroMq(CreateClientZeroMq {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::ZeroMq(ZeroMqIngestorStartPlan {
                client: client(),
                mode: nervix_models::ZeroMqIngestMode::NoAckSequential,
            }),
            reads_headers: false,
        }
    }

    fn sqs_mode() -> SqsIngestMode {
        SqsIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: retry_policy(),
        }
    }

    fn sqs_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Sqs {
                client: named("upstream"),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: sqs_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientSqs(CreateClientSqs {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Sqs(SqsIngestorStartPlan {
                client: client(),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: sqs_mode(),
            }),
            reads_headers: true,
        }
    }

    fn endpoint_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Endpoint {
                endpoint: named("upstream"),
                mode: nervix_models::EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            },
            source_model: Model::Endpoint(CreateEndpoint {
                name: named("upstream"),
                on_vhost: named("public"),
                path: "/events".to_string(),
                endpoint_type: EndpointType::Http,
                signaling_protocol: None,
            }),
            plan: SourceStartPlan::Endpoint(EndpointIngestorStartPlan {
                endpoint: named("upstream"),
                mode: nervix_models::EndpointIngestMode::NoAckSequential,
            }),
            reads_headers: true,
        }
    }

    fn websockets_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Websockets {
                client: named("upstream"),
                mode: nervix_models::WebsocketsIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Drop,
            },
            source_model: Model::ClientWebsockets(CreateClientWebsockets {
                name: named("upstream"),
                mount: None,
                signaling_protocol: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Websockets(WebsocketsIngestorStartPlan {
                client: client(),
                mode: nervix_models::WebsocketsIngestMode::NoAckSequential,
                signaling_protocol: None,
            }),
            reads_headers: false,
        }
    }

    fn syslog_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Syslog {
                client: named("upstream"),
                quiesce: IngestQuiesceMode::Suspend,
            },
            source_model: Model::ClientSyslog(CreateClientSyslog {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: SourceStartPlan::Syslog(SyslogIngestorStartPlan { client: client() }),
            reads_headers: false,
        }
    }

    #[rstest]
    #[case::http(http_case())]
    #[case::kafka(kafka_case())]
    #[case::pulsar(pulsar_case())]
    #[case::mqtt(mqtt_case())]
    #[case::nats(nats_case())]
    #[case::rabbitmq(rabbitmq_case())]
    #[case::redis_pubsub(redis_pubsub_case())]
    #[case::prometheus(prometheus_case())]
    #[case::zeromq(zeromq_case())]
    #[case::sqs(sqs_case())]
    #[case::endpoint(endpoint_case())]
    #[case::websockets(websockets_case())]
    #[case::syslog(syslog_case())]
    fn decides_the_source_plan_of_each_source_kind(#[case] case: SourceCase) {
        let SourceCase {
            source,
            source_model,
            plan: expected_plan,
            reads_headers,
        } = case;
        let declared = source.clone();

        let plans = planned_entrypoints(domain_models(source, source_model))
            .assured("a matching source and ingestor produce a plan");
        let plan = start_plan(&plans);

        assert_eq!(plan.source, expected_plan);
        assert_eq!(plan.ingestor.name, named("source"));
        assert_eq!(plan.ingestor.routes[0].relay, named("events"));
        assert_eq!(
            plan.ingestor.declared_source.transport(),
            declared.transport_kind()
        );
        assert_eq!(plan.ingestor.reads_headers(), reads_headers);
        assert_eq!(
            plan.ingestor.declared_source.quiesce_mode(),
            declared.quiesce()
        );
        assert_eq!(plan.acknowledgement(), declared.acknowledgement());
        assert!(!plan.keeps_domain_offsets());
    }

    #[test]
    fn plans_kafka_domain_offsets_for_the_scheduled_primary() {
        let source = IngestSource::Kafka {
            client: named("upstream"),
            topic: named("events"),
            offset_mode: KafkaOffsetMode::Domain,
            instances: nonzero!(2u64),
            mode: KafkaIngestMode::NoAckParallel,
            quiesce: IngestQuiesceMode::Suspend,
        };
        let domain = named::<DomainName>("sales");
        let mut nodes = unplaced_schedule(domain_models(source, kafka_case().source_model));
        nodes
            .get_mut(&NodeRef::new(
                nervix_models::ModelKind::Ingestor,
                named::<ModelName>("source"),
            ))
            .assured("the fixture schedules the ingestor")
            .primary_node = Some(named("node-a"));
        let activation =
            crate::registry::DomainActivationPlan::from_scheduled_nodes(&domain, &nodes)
                .assured("the fixture surfaces resolve");
        let plans = EntrypointPlans::from_scheduled_nodes(&domain, &nodes, &activation)
            .assured("a domain-offset Kafka ingestor produces a plan");
        let plan = start_plan(&plans);

        let domain_offsets = KafkaOffsetPlan::Domain(KafkaDomainOffsetPlacement {
            primary_node: Some(named("node-a")),
        });
        assert!(matches!(
            &plan.source,
            SourceStartPlan::Kafka(source) if source.offsets == domain_offsets
        ));
        assert!(plan.keeps_domain_offsets());
    }

    #[test]
    fn answers_quiesce_support_from_the_source_vocabulary() {
        let case = mqtt_case();
        let plans = planned_entrypoints(domain_models(case.source, case.source_model))
            .assured("a matching source and ingestor produce a plan");
        let plan = start_plan(&plans);

        // A clean, at-most-once MQTT session has nothing the broker would hold for it, so the
        // vocabulary declares that it cannot suspend while it may still drop.
        assert!(
            !plan
                .ingestor
                .declared_source
                .supports_quiesce(&IngestQuiesceMode::Suspend)
        );
        assert!(
            plan.ingestor
                .declared_source
                .supports_quiesce(&IngestQuiesceMode::Drop)
        );
    }

    #[rstest::rstest]
    #[case::client(
        http_case(),
        Model::ClientHttp(CreateClientHttp {
            name: named("different"),
            mount: None,
            config: Vec::new(),
        })
    )]
    #[case::endpoint(
        endpoint_case(),
        Model::Endpoint(CreateEndpoint {
            name: named("different"),
            on_vhost: named("public"),
            path: "/events".to_string(),
            endpoint_type: EndpointType::Http,
            signaling_protocol: None,
        })
    )]
    fn rejects_a_resolved_source_with_the_wrong_identity(
        #[case] case: SourceCase,
        #[case] different: Model,
    ) {
        let ingestor = CreateIngestor {
            name: named("source"),
            output_routes: ProcessorOutputs::single(named("events")),
            decode_using_codec: named("json"),
            timestamp_source: None,
            source: case.source.clone(),
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };

        let error = SourceStartPlan::decide(
            &ingestor,
            &different,
            &nervix_models::ScheduledNode::new(
                ingestor_model(case.source),
                nervix_models::SchemaFingerprint::from_digest([1; 32]),
            ),
        )
        .expect_err("a differently named source must not be planned");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::SourceIdentityMismatch {
                ingestor: named("source"),
                expected: named("upstream"),
                resolved: named("different"),
            }
        );
    }

    #[test]
    fn rejects_a_resolved_source_of_another_kind() {
        let mut models = domain_models(http_case().source, http_case().source_model);
        // The HTTP source names the client `upstream`; schedule a Kafka client under that name.
        models.retain(|model| !matches!(model, Model::ClientHttp(_)));
        models.push(kafka_case().source_model);

        let error =
            planned_entrypoints(models).expect_err("a Kafka client cannot serve an HTTP source");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::SourceKindMismatch {
                ingestor: named("source"),
                resolved: nervix_models::ModelKind::Client,
            }
        );
    }

    #[test]
    fn rejects_an_ingestor_whose_source_is_not_scheduled() {
        let mut models = domain_models(http_case().source, http_case().source_model);
        models.retain(|model| !matches!(model, Model::ClientHttp(_)));

        let error = planned_entrypoints(models).expect_err("the source is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingSource {
                ingestor: named("source"),
                kind: nervix_models::ModelKind::Client,
                reference: named("upstream"),
            }
        );
    }

    #[test]
    fn rejects_a_node_filter_that_does_not_lower() {
        let mut models = domain_models(http_case().source, http_case().source_model);
        for model in &mut models {
            if let Model::Ingestor(ingestor) = model {
                ingestor.filter_where = Some(unlowerable_predicate());
            }
        }

        let error = planned_entrypoints(models).expect_err("the filter does not lower");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InvalidFilter {
                kind: nervix_models::ModelKind::Ingestor,
                node: named("source"),
                operation: MessageErrorOperation::FilterWhere,
            }
        );
    }

    #[test]
    fn rejects_an_ingestor_whose_codec_is_not_scheduled() {
        let mut models = domain_models(http_case().source, http_case().source_model);
        models.retain(|model| !matches!(model, Model::Codec(_)));

        let error = planned_entrypoints(models).expect_err("the codec is required");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::MissingCodec {
                ingestor: named("source"),
                codec: named("json"),
            }
        );
    }
}
