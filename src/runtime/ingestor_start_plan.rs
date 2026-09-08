//! Decides the complete specification for starting one scheduled ingestor.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an ingestor and its source model into one typed start outcome.
//! - **Depends on.** Scheduled control-plane Models and runtime vocabulary values.
//! - **Must not know.** Tokio, locks, shared maps, connector I/O, or task spawning.

use error_stack::Report;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorRouteSpec {
    pub(super) relay: RelayName,
    pub(super) construction: RouteConstruction,
    pub(super) flush_policy: Option<FlushPolicy>,
    pub(super) message_error_policy: MessageErrorPolicy,
    pub(super) branch: Option<OutputBranch>,
}

impl From<&ProcessorOutput> for IngestorRouteSpec {
    fn from(route: &ProcessorOutput) -> Self {
        Self {
            relay: route.relay.clone(),
            construction: route.construction.clone(),
            flush_policy: route.flush_policy.clone(),
            message_error_policy: route.message_error_policy.clone(),
            branch: route.branch.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestorQuiesceSupport {
    Suspend,
    Mqtt { suspend: bool },
    BufferOrDrop,
    SuspendBufferOrDrop,
    SuspendOrBuffer,
    Endpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorQuiescePlan {
    mode: IngestQuiesceMode,
    support: IngestorQuiesceSupport,
}

impl IngestorQuiescePlan {
    pub(super) fn mode(&self) -> &IngestQuiesceMode {
        &self.mode
    }

    pub(super) fn supports(&self, mode: &IngestQuiesceMode) -> bool {
        match self.support {
            IngestorQuiesceSupport::Suspend => matches!(mode, IngestQuiesceMode::Suspend),
            IngestorQuiesceSupport::Mqtt { suspend } => match mode {
                IngestQuiesceMode::Suspend => suspend,
                IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop => true,
                IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. } => {
                    false
                }
            },
            IngestorQuiesceSupport::BufferOrDrop => matches!(
                mode,
                IngestQuiesceMode::Buffer { .. } | IngestQuiesceMode::Drop
            ),
            IngestorQuiesceSupport::SuspendBufferOrDrop => matches!(
                mode,
                IngestQuiesceMode::Suspend
                    | IngestQuiesceMode::Buffer { .. }
                    | IngestQuiesceMode::Drop
            ),
            IngestorQuiesceSupport::SuspendOrBuffer => matches!(
                mode,
                IngestQuiesceMode::Suspend | IngestQuiesceMode::Buffer { .. }
            ),
            IngestorQuiesceSupport::Endpoint => matches!(
                mode,
                IngestQuiesceMode::EndpointBuffer { .. } | IngestQuiesceMode::Reject { .. }
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorSpec {
    pub(super) domain: DomainName,
    pub(super) name: IngestorName,
    pub(super) routes: Vec<IngestorRouteSpec>,
    pub(super) decode_using_codec: CodecName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) general_error_policy: GeneralErrorPolicy,
    pub(super) filter_where: Option<nervix_models::Expression>,
    pub(super) metadata_kind: IngestMetadataKind,
    pub(super) allow_header_reads: bool,
    pub(super) quiesce: IngestorQuiescePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorClientSpec {
    pub(super) mount: Option<ResourceName>,
    pub(super) config: Vec<ClientConfigEntry>,
}

macro_rules! client_plan {
    ($name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub(super) struct $name {
            pub(super) ingestor: IngestorSpec,
            pub(super) client: IngestorClientSpec,
            $(pub(super) $field: $type,)*
        }
    };
}

client_plan!(HttpIngestorStartPlan { every: String });
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KafkaOffsetStatePlacement {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) primary_node: Option<ClusterNodeName>,
}

client_plan!(KafkaIngestorStartPlan {
    topic: nervix_models::TopicName,
    offset_mode: KafkaOffsetMode,
    instances: NonZeroU64,
    mode: KafkaIngestMode,
    offset_state_placement: Option<KafkaOffsetStatePlacement>,
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
    every: String,
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
pub(super) struct EndpointIngestorStartPlan {
    pub(super) ingestor: IngestorSpec,
    pub(super) endpoint: EndpointName,
    pub(super) mode: nervix_models::EndpointIngestMode,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IngestorConnectorKind {
    Http,
    Kafka,
    Pulsar,
    Mqtt,
    Nats,
    RabbitMq,
    RedisPubSub,
    Prometheus,
    ZeroMq,
    Sqs,
    Endpoint,
    Websockets,
    Syslog,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum IngestorStartPlan {
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum IngestorStartPlanError {
    #[error("scheduled node is not an ingestor")]
    NotIngestor,
    #[error("scheduled ingestor identity does not match its configuration")]
    IngestorIdentityMismatch,
    #[error("resolved source kind does not match the ingestor source")]
    SourceKindMismatch,
    #[error("resolved source identity does not match the ingestor source reference")]
    SourceIdentityMismatch,
}

impl IngestorStartPlan {
    pub(super) fn decide(
        domain: &DomainName,
        scheduled: &ScheduledNode,
        source_model: &Model,
    ) -> Result<Self, Report<IngestorStartPlanError>> {
        let Model::Ingestor(ingestor) = scheduled.config.as_ref() else {
            return Err(Report::new(IngestorStartPlanError::NotIngestor));
        };
        if scheduled.identifier != ModelName::from(&ingestor.name) {
            return Err(Report::new(
                IngestorStartPlanError::IngestorIdentityMismatch,
            ));
        }
        Self::decide_ingestor(domain, ingestor, source_model, Some(scheduled))
    }

    pub(super) fn decide_unscheduled(
        domain: &DomainName,
        ingestor: &CreateIngestor,
        source_model: &Model,
    ) -> Result<Self, Report<IngestorStartPlanError>> {
        Self::decide_ingestor(domain, ingestor, source_model, None)
    }

    fn decide_ingestor(
        domain: &DomainName,
        ingestor: &CreateIngestor,
        source_model: &Model,
        scheduled: Option<&ScheduledNode>,
    ) -> Result<Self, Report<IngestorStartPlanError>> {
        let routes = ingestor
            .output_routes
            .outputs()
            .map(IngestorRouteSpec::from)
            .collect::<Vec<_>>();
        let common = |metadata_kind, allow_header_reads, mode, support| IngestorSpec {
            domain: domain.clone(),
            name: ingestor.name.clone(),
            routes: routes.clone(),
            decode_using_codec: ingestor.decode_using_codec.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            general_error_policy: ingestor.general_error_policy.clone(),
            filter_where: ingestor.filter_where.clone(),
            metadata_kind,
            allow_header_reads,
            quiesce: IngestorQuiescePlan { mode, support },
        };
        let client = |expected: &ClientName,
                      actual: &ClientName,
                      mount: &Option<ResourceName>,
                      config: &[ClientConfigEntry]| {
            if expected != actual {
                return Err(Report::new(IngestorStartPlanError::SourceIdentityMismatch));
            }
            Ok(IngestorClientSpec {
                mount: mount.clone(),
                config: config.to_vec(),
            })
        };

        match (&ingestor.source, source_model) {
            (
                IngestSource::Http {
                    client: expected,
                    every,
                    quiesce,
                },
                Model::ClientHttp(resolved),
            ) => Ok(Self::Http(HttpIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::SuspendOrBuffer,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                every: every.clone(),
            })),
            (
                IngestSource::Kafka {
                    client: expected,
                    topic,
                    offset_mode,
                    instances,
                    mode,
                    quiesce,
                },
                Model::ClientKafka(resolved),
            ) => Ok(Self::Kafka(KafkaIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Kafka,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::Suspend,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                topic: topic.clone(),
                offset_mode: offset_mode.clone(),
                instances: *instances,
                mode: mode.clone(),
                offset_state_placement: if matches!(offset_mode, KafkaOffsetMode::Domain) {
                    scheduled.map(|node| KafkaOffsetStatePlacement {
                        placement: RuntimeStatePlacement {
                            domain: domain.clone(),
                            state: RuntimeStateKind::KafkaOffset,
                            kind: node.kind(),
                            identifier: node.identifier.clone(),
                            schema_fingerprint: [0; 32],
                            branch_key: None,
                        },
                        primary_node: node.primary_node.clone(),
                    })
                } else {
                    None
                },
            })),
            (
                IngestSource::Pulsar {
                    client: expected,
                    topic,
                    subscription,
                    instances,
                    mode,
                    quiesce,
                },
                Model::ClientPulsar(resolved),
            ) => Ok(Self::Pulsar(PulsarIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::Suspend,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
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
                    quiesce,
                },
                Model::ClientMqtt(resolved),
            ) => Ok(Self::Mqtt(MqttIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::Mqtt {
                        suspend: mode.session() == MqttSession::Persistent
                            && mode.qos() == MqttQos::AtLeastOnce,
                    },
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
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
                    quiesce,
                },
                Model::ClientNats(resolved),
            ) => Ok(Self::Nats(NatsIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::BufferOrDrop,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
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
                    quiesce,
                },
                Model::ClientRabbitMq(resolved),
            ) => Ok(Self::RabbitMq(RabbitMqIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::Suspend,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                queue: queue.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::RedisPubSub {
                    client: expected,
                    channel,
                    mode,
                    quiesce,
                },
                Model::ClientRedis(resolved),
            ) => Ok(Self::RedisPubSub(RedisPubSubIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::BufferOrDrop,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                channel: channel.clone(),
                mode: mode.clone(),
            })),
            (
                IngestSource::Prometheus {
                    client: expected,
                    query,
                    every,
                    quiesce,
                },
                Model::ClientPrometheus(resolved),
            ) => Ok(Self::Prometheus(PrometheusIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::SuspendOrBuffer,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                query: query.clone(),
                every: every.clone(),
            })),
            (
                IngestSource::ZeroMq {
                    client: expected,
                    mode,
                    quiesce,
                },
                Model::ClientZeroMq(resolved),
            ) => Ok(Self::ZeroMq(ZeroMqIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::SuspendBufferOrDrop,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                mode: mode.clone(),
            })),
            (
                IngestSource::Sqs {
                    client: expected,
                    queue,
                    instances,
                    mode,
                    quiesce,
                },
                Model::ClientSqs(resolved),
            ) => Ok(Self::Sqs(SqsIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    true,
                    quiesce.clone(),
                    IngestorQuiesceSupport::Suspend,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                queue: queue.clone(),
                instances: *instances,
                mode: mode.clone(),
            })),
            (
                IngestSource::Endpoint {
                    endpoint: expected,
                    mode,
                    quiesce,
                },
                Model::Endpoint(resolved),
            ) => {
                if expected != &resolved.name {
                    return Err(Report::new(IngestorStartPlanError::SourceIdentityMismatch));
                }
                Ok(Self::Endpoint(EndpointIngestorStartPlan {
                    ingestor: common(
                        IngestMetadataKind::Headers,
                        true,
                        quiesce.clone(),
                        IngestorQuiesceSupport::Endpoint,
                    ),
                    endpoint: resolved.name.clone(),
                    mode: mode.clone(),
                }))
            }
            (
                IngestSource::Websockets {
                    client: expected,
                    mode,
                    quiesce,
                },
                Model::ClientWebsockets(resolved),
            ) => Ok(Self::Websockets(WebsocketsIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Headers,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::BufferOrDrop,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
                mode: mode.clone(),
                signaling_protocol: resolved.signaling_protocol.clone(),
            })),
            (
                IngestSource::Syslog {
                    client: expected,
                    quiesce,
                },
                Model::ClientSyslog(resolved),
            ) => Ok(Self::Syslog(SyslogIngestorStartPlan {
                ingestor: common(
                    IngestMetadataKind::Syslog,
                    false,
                    quiesce.clone(),
                    IngestorQuiesceSupport::SuspendBufferOrDrop,
                ),
                client: client(expected, &resolved.name, &resolved.mount, &resolved.config)?,
            })),
            _ => Err(Report::new(IngestorStartPlanError::SourceKindMismatch)),
        }
    }

    #[cfg(test)]
    pub(super) fn connector_kind(&self) -> IngestorConnectorKind {
        match self {
            Self::Http(_) => IngestorConnectorKind::Http,
            Self::Kafka(_) => IngestorConnectorKind::Kafka,
            Self::Pulsar(_) => IngestorConnectorKind::Pulsar,
            Self::Mqtt(_) => IngestorConnectorKind::Mqtt,
            Self::Nats(_) => IngestorConnectorKind::Nats,
            Self::RabbitMq(_) => IngestorConnectorKind::RabbitMq,
            Self::RedisPubSub(_) => IngestorConnectorKind::RedisPubSub,
            Self::Prometheus(_) => IngestorConnectorKind::Prometheus,
            Self::ZeroMq(_) => IngestorConnectorKind::ZeroMq,
            Self::Sqs(_) => IngestorConnectorKind::Sqs,
            Self::Endpoint(_) => IngestorConnectorKind::Endpoint,
            Self::Websockets(_) => IngestorConnectorKind::Websockets,
            Self::Syslog(_) => IngestorConnectorKind::Syslog,
        }
    }

    pub(super) fn ingestor(&self) -> &IngestorSpec {
        match self {
            Self::Http(plan) => &plan.ingestor,
            Self::Kafka(plan) => &plan.ingestor,
            Self::Pulsar(plan) => &plan.ingestor,
            Self::Mqtt(plan) => &plan.ingestor,
            Self::Nats(plan) => &plan.ingestor,
            Self::RabbitMq(plan) => &plan.ingestor,
            Self::RedisPubSub(plan) => &plan.ingestor,
            Self::Prometheus(plan) => &plan.ingestor,
            Self::ZeroMq(plan) => &plan.ingestor,
            Self::Sqs(plan) => &plan.ingestor,
            Self::Endpoint(plan) => &plan.ingestor,
            Self::Websockets(plan) => &plan.ingestor,
            Self::Syslog(plan) => &plan.ingestor,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).expect("fixture name must be valid")
    }

    fn scheduled_ingestor(source: IngestSource) -> ScheduledNode {
        let ingestor = CreateIngestor {
            name: named("source"),
            output_routes: nervix_models::ProcessorOutputs::single(named("events")),
            decode_using_codec: named("json"),
            timestamp_source: None,
            source,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        };
        ScheduledNode::new(Model::Ingestor(ingestor))
    }

    fn instances() -> NonZeroU64 {
        NonZeroU64::new(1).expect("one is non-zero")
    }

    fn retry_policy() -> nervix_models::RetryPolicy {
        nervix_models::RetryPolicy {
            backoff: "100ms".to_string(),
            max_backoff: "1s".to_string(),
        }
    }

    fn client_fields() -> (ClientName, Option<ResourceName>, Vec<ClientConfigEntry>) {
        (named("upstream"), None, Vec::new())
    }

    fn connector_case(kind: IngestorConnectorKind) -> (IngestSource, Model) {
        let (client, mount, config) = client_fields();
        match kind {
            IngestorConnectorKind::Http => (
                IngestSource::Http {
                    client,
                    every: "1s".to_string(),
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientHttp(CreateClientHttp {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Kafka => (
                IngestSource::Kafka {
                    client,
                    topic: named("events"),
                    offset_mode: KafkaOffsetMode::Domain,
                    instances: instances(),
                    mode: KafkaIngestMode::NoAckParallel,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientKafka(CreateClientKafka {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Pulsar => (
                IngestSource::Pulsar {
                    client,
                    topic: named("events"),
                    subscription: named("nervix"),
                    instances: instances(),
                    mode: PulsarIngestMode::NoAckParallel,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientPulsar(CreateClientPulsar {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Mqtt => (
                IngestSource::Mqtt {
                    client,
                    topic: "events/#".to_string(),
                    instances: instances(),
                    mode: MqttIngestMode::NoAckSequential {
                        session: MqttSession::Clean,
                        qos: MqttQos::AtMostOnce,
                    },
                    quiesce: IngestQuiesceMode::Drop,
                },
                Model::ClientMqtt(CreateClientMqtt {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Nats => (
                IngestSource::Nats {
                    client,
                    subject: named("events"),
                    queue_group: named("nervix"),
                    instances: instances(),
                    mode: nervix_models::NatsIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Drop,
                },
                Model::ClientNats(CreateClientNats {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::RabbitMq => (
                IngestSource::RabbitMq {
                    client,
                    queue: named("events"),
                    instances: instances(),
                    mode: RabbitMqIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: retry_policy(),
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientRabbitMq(CreateClientRabbitMq {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::RedisPubSub => (
                IngestSource::RedisPubSub {
                    client,
                    channel: named("events"),
                    mode: nervix_models::RedisPubSubIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Drop,
                },
                Model::ClientRedis(CreateClientRedis {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Prometheus => (
                IngestSource::Prometheus {
                    client,
                    query: "up".to_string(),
                    every: "1s".to_string(),
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientPrometheus(CreateClientPrometheus {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::ZeroMq => (
                IngestSource::ZeroMq {
                    client,
                    mode: nervix_models::ZeroMqIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientZeroMq(CreateClientZeroMq {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Sqs => (
                IngestSource::Sqs {
                    client,
                    queue: named("events"),
                    instances: instances(),
                    mode: SqsIngestMode::AckSequential {
                        timeout: "5s".to_string(),
                        retry_policy: retry_policy(),
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientSqs(CreateClientSqs {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
            IngestorConnectorKind::Endpoint => (
                IngestSource::Endpoint {
                    endpoint: named("upstream"),
                    mode: nervix_models::EndpointIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::EndpointBuffer {
                        max_size: "1MiB".to_string(),
                    },
                },
                Model::Endpoint(nervix_models::CreateEndpoint {
                    name: named("upstream"),
                    on_vhost: named("public"),
                    path: "/events".to_string(),
                    endpoint_type: EndpointType::Http,
                    signaling_protocol: None,
                }),
            ),
            IngestorConnectorKind::Websockets => (
                IngestSource::Websockets {
                    client,
                    mode: nervix_models::WebsocketsIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Drop,
                },
                Model::ClientWebsockets(CreateClientWebsockets {
                    name: named("upstream"),
                    mount,
                    signaling_protocol: None,
                    config,
                }),
            ),
            IngestorConnectorKind::Syslog => (
                IngestSource::Syslog {
                    client,
                    quiesce: IngestQuiesceMode::Suspend,
                },
                Model::ClientSyslog(CreateClientSyslog {
                    name: named("upstream"),
                    mount,
                    config,
                }),
            ),
        }
    }

    #[rstest]
    #[case::http(IngestorConnectorKind::Http)]
    #[case::kafka(IngestorConnectorKind::Kafka)]
    #[case::pulsar(IngestorConnectorKind::Pulsar)]
    #[case::mqtt(IngestorConnectorKind::Mqtt)]
    #[case::nats(IngestorConnectorKind::Nats)]
    #[case::rabbitmq(IngestorConnectorKind::RabbitMq)]
    #[case::redis_pubsub(IngestorConnectorKind::RedisPubSub)]
    #[case::prometheus(IngestorConnectorKind::Prometheus)]
    #[case::zeromq(IngestorConnectorKind::ZeroMq)]
    #[case::sqs(IngestorConnectorKind::Sqs)]
    #[case::endpoint(IngestorConnectorKind::Endpoint)]
    #[case::websockets(IngestorConnectorKind::Websockets)]
    #[case::syslog(IngestorConnectorKind::Syslog)]
    fn decides_each_connector_kind(#[case] expected: IngestorConnectorKind) {
        let (source, source_model) = connector_case(expected);
        let scheduled = scheduled_ingestor(source);

        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");

        assert_eq!(plan.connector_kind(), expected);
        assert_eq!(plan.ingestor().name, named("source"));
        assert_eq!(plan.ingestor().routes[0].relay, named("events"));
    }

    #[rstest]
    fn decides_http_connector_from_the_scheduled_ingestor_and_resolved_client() {
        let scheduled = scheduled_ingestor(IngestSource::Http {
            client: named("upstream"),
            every: "1s".to_string(),
            quiesce: IngestQuiesceMode::Suspend,
        });
        let source_model = Model::ClientHttp(CreateClientHttp {
            name: named("upstream"),
            mount: None,
            config: Vec::new(),
        });

        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");

        assert_eq!(plan.connector_kind(), IngestorConnectorKind::Http);
        assert_eq!(plan.ingestor().routes[0].relay, named("events"));
        assert_eq!(plan.ingestor().quiesce.mode(), &IngestQuiesceMode::Suspend);
    }

    #[test]
    fn plans_kafka_domain_offset_state_for_the_scheduled_primary() {
        let (source, source_model) = connector_case(IngestorConnectorKind::Kafka);
        let mut scheduled = scheduled_ingestor(source);
        scheduled.primary_node = Some(named("node-a"));

        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");
        let IngestorStartPlan::Kafka(plan) = plan else {
            panic!("Kafka inputs must produce a Kafka plan");
        };
        let offset = plan
            .offset_state_placement
            .expect("domain offsets require a state placement");

        assert_eq!(offset.primary_node, Some(named("node-a")));
        assert_eq!(offset.placement.domain, named("sales"));
        assert_eq!(offset.placement.state, RuntimeStateKind::KafkaOffset);
        assert_eq!(
            offset.placement.identifier,
            ModelName::from(&plan.ingestor.name)
        );
    }

    #[test]
    fn captures_source_quiesce_capabilities_in_the_plan() {
        let (source, source_model) = connector_case(IngestorConnectorKind::Mqtt);
        let scheduled = scheduled_ingestor(source);
        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");

        assert!(
            !plan
                .ingestor()
                .quiesce
                .supports(&IngestQuiesceMode::Suspend)
        );
        assert!(plan.ingestor().quiesce.supports(&IngestQuiesceMode::Drop));
    }

    #[test]
    fn rejects_a_resolved_client_with_the_wrong_identity() {
        let (source, mut source_model) = connector_case(IngestorConnectorKind::Http);
        let Model::ClientHttp(client) = &mut source_model else {
            panic!("HTTP case must contain an HTTP client");
        };
        client.name = named("different");

        let error =
            IngestorStartPlan::decide(&named("sales"), &scheduled_ingestor(source), &source_model)
                .expect_err("a differently named client must not be planned");

        assert_eq!(
            error.current_context(),
            &IngestorStartPlanError::SourceIdentityMismatch
        );
    }
}
