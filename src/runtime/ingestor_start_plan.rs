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

/// The quiesce mode an ingestor declares and the modes its source honors.
///
/// Both come from the source vocabulary, which is the one declaration of which quiesce modes a
/// source supports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorQuiescePlan {
    source: IngestSource,
}

impl IngestorQuiescePlan {
    pub(super) fn mode(&self) -> &IngestQuiesceMode {
        self.source.quiesce()
    }

    pub(super) fn supports(&self, mode: &IngestQuiesceMode) -> bool {
        self.source.supports_quiesce(mode)
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
    pub(super) mount: Option<ClientResourceMount>,
    pub(super) config: Vec<ClientConfigEntry>,
}

impl IngestorClientSpec {
    /// The client a source reads through, once the client it resolved to is the one its statement
    /// names.
    fn resolved(
        expected: &ClientName,
        resolved: &ClientName,
        mount: Option<&ClientResourceMount>,
        config: &[ClientConfigEntry],
    ) -> Result<Self, Report<IngestorStartPlanError>> {
        if expected != resolved {
            return Err(Report::new(IngestorStartPlanError::SourceIdentityMismatch));
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
        pub(super) struct $name {
            pub(super) client: IngestorClientSpec,
            $(pub(super) $field: $type,)*
        }
    };
}

client_plan!(HttpIngestorStartPlan {
    every: nervix_models::DomainClockPeriod,
});
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
pub(super) struct EndpointIngestorStartPlan {
    pub(super) endpoint: EndpointName,
    pub(super) mode: nervix_models::EndpointIngestMode,
}

/// Everything the host needs to start one ingestor: the ingestor itself and the plan of the source
/// it reads from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IngestorStartPlan {
    pub(super) ingestor: IngestorSpec,
    pub(super) source: SourceStartPlan,
}

/// The plan of one ingestor's source, which the composition root maps to the connector that runs
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SourceStartPlan {
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
        let source = SourceStartPlan::decide(domain, &ingestor.source, source_model, scheduled)?;
        let routes = ingestor
            .output_routes
            .outputs()
            .map(IngestorRouteSpec::from)
            .collect::<Vec<_>>();
        let ingestor = IngestorSpec {
            domain: domain.clone(),
            name: ingestor.name.clone(),
            routes,
            decode_using_codec: ingestor.decode_using_codec.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            general_error_policy: ingestor.general_error_policy.clone(),
            filter_where: ingestor.filter_where.clone(),
            metadata_kind: source.metadata_kind(),
            allow_header_reads: ingestor.source.reads_headers(),
            quiesce: IngestorQuiescePlan {
                source: ingestor.source.clone(),
            },
        };
        Ok(Self { ingestor, source })
    }
}

impl SourceStartPlan {
    fn decide(
        domain: &DomainName,
        source: &IngestSource,
        source_model: &Model,
        scheduled: Option<&ScheduledNode>,
    ) -> Result<Self, Report<IngestorStartPlanError>> {
        match (source, source_model) {
            (
                IngestSource::Http {
                    client: expected,
                    every,
                    ..
                },
                Model::ClientHttp(resolved),
            ) => Ok(Self::Http(HttpIngestorStartPlan {
                client: IngestorClientSpec::resolved(
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
                let offset_state_placement = if matches!(offset_mode, KafkaOffsetMode::Domain) {
                    scheduled.map(|node| KafkaOffsetStatePlacement {
                        placement: RuntimeStatePlacement {
                            domain: domain.clone(),
                            state: RuntimeState::KafkaOffset,
                            kind: node.kind(),
                            identifier: node.identifier.clone(),
                            branch_key: None,
                        },
                        primary_node: node.primary_node.clone(),
                    })
                } else {
                    None
                };
                Ok(Self::Kafka(KafkaIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    topic: topic.clone(),
                    offset_mode: offset_mode.clone(),
                    instances: *instances,
                    mode: mode.clone(),
                    offset_state_placement,
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
                    return Err(Report::new(IngestorStartPlanError::SourceIdentityMismatch));
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
                    expected,
                    &resolved.name,
                    resolved.mount.as_ref(),
                    &resolved.config,
                )?,
            })),
            _ => Err(Report::new(IngestorStartPlanError::SourceKindMismatch)),
        }
    }

    /// The metadata namespace the source's messages expose to the ingestor's programs.
    fn metadata_kind(&self) -> IngestMetadataKind {
        match self {
            Self::Kafka(_) => IngestMetadataKind::Kafka,
            Self::Syslog(_) => IngestMetadataKind::Syslog,
            Self::Http(_)
            | Self::Pulsar(_)
            | Self::Mqtt(_)
            | Self::Nats(_)
            | Self::RabbitMq(_)
            | Self::RedisPubSub(_)
            | Self::Prometheus(_)
            | Self::ZeroMq(_)
            | Self::Sqs(_)
            | Self::Endpoint(_)
            | Self::Websockets(_) => IngestMetadataKind::Headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClientPoolBounds, MqttQos, MqttSession};
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;

    /// Pool bounds for the client fixtures, whose subject is the ingestor start plan rather than
    /// the declared capacity.
    fn pool_bounds() -> ClientPoolBounds {
        ClientPoolBounds::new(1, nonzero!(4u32)).assured("one does not exceed four")
    }

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
        ScheduledNode::new(
            Model::Ingestor(ingestor),
            SchemaFingerprint::from_digest([1; 32]),
        )
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

    fn cadence() -> nervix_models::DomainClockPeriod {
        "1s".parse()
            .assured("the fixture cadence is a positive duration")
    }

    /// The client every client-backed fixture resolves to.
    fn client() -> IngestorClientSpec {
        IngestorClientSpec {
            mount: None,
            config: Vec::new(),
        }
    }

    /// One source statement, the model it resolves to, and the source plan, metadata namespace and
    /// header access they decide.
    struct SourceCase {
        source: IngestSource,
        source_model: Model,
        plan: SourceStartPlan,
        metadata_kind: IngestMetadataKind,
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
            metadata_kind: IngestMetadataKind::Headers,
            reads_headers: true,
        }
    }

    fn kafka_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Kafka {
                client: named("upstream"),
                topic: named("events"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(named("nervix")),
                instances: instances(),
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
                offset_mode: KafkaOffsetMode::ConsumerGroup(named("nervix")),
                instances: instances(),
                mode: KafkaIngestMode::NoAckParallel,
                offset_state_placement: None,
            }),
            metadata_kind: IngestMetadataKind::Kafka,
            reads_headers: true,
        }
    }

    fn pulsar_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Pulsar {
                client: named("upstream"),
                topic: named("events"),
                subscription: named("nervix"),
                instances: instances(),
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
                instances: instances(),
                mode: PulsarIngestMode::NoAckParallel,
            }),
            metadata_kind: IngestMetadataKind::Headers,
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
                instances: instances(),
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
                instances: instances(),
                mode: mqtt_mode(),
            }),
            metadata_kind: IngestMetadataKind::Headers,
            reads_headers: false,
        }
    }

    fn nats_case() -> SourceCase {
        SourceCase {
            source: IngestSource::Nats {
                client: named("upstream"),
                subject: named("events"),
                queue_group: named("nervix"),
                instances: instances(),
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
                instances: instances(),
                mode: nervix_models::NatsIngestMode::NoAckSequential,
            }),
            metadata_kind: IngestMetadataKind::Headers,
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
                instances: instances(),
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
                instances: instances(),
                mode: rabbitmq_mode(),
            }),
            metadata_kind: IngestMetadataKind::Headers,
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
            metadata_kind: IngestMetadataKind::Headers,
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
            metadata_kind: IngestMetadataKind::Headers,
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
            metadata_kind: IngestMetadataKind::Headers,
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
                instances: instances(),
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
                instances: instances(),
                mode: sqs_mode(),
            }),
            metadata_kind: IngestMetadataKind::Headers,
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
            source_model: Model::Endpoint(nervix_models::CreateEndpoint {
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
            metadata_kind: IngestMetadataKind::Headers,
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
            metadata_kind: IngestMetadataKind::Headers,
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
            metadata_kind: IngestMetadataKind::Syslog,
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
            metadata_kind,
            reads_headers,
        } = case;
        let declared_quiesce = source.quiesce().clone();
        let scheduled = scheduled_ingestor(source);

        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");

        assert_eq!(plan.source, expected_plan);
        assert_eq!(plan.ingestor.name, named("source"));
        assert_eq!(plan.ingestor.routes[0].relay, named("events"));
        assert_eq!(plan.ingestor.metadata_kind, metadata_kind);
        assert_eq!(plan.ingestor.allow_header_reads, reads_headers);
        assert_eq!(plan.ingestor.quiesce.mode(), &declared_quiesce);
    }

    #[test]
    fn plans_kafka_domain_offset_state_for_the_scheduled_primary() {
        let mut scheduled = scheduled_ingestor(IngestSource::Kafka {
            client: named("upstream"),
            topic: named("events"),
            offset_mode: KafkaOffsetMode::Domain,
            instances: instances(),
            mode: KafkaIngestMode::NoAckParallel,
            quiesce: IngestQuiesceMode::Suspend,
        });
        scheduled.primary_node = Some(named("node-a"));
        let source_model = kafka_case().source_model;

        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &source_model)
            .expect("matching scheduled inputs must produce a plan");
        let SourceStartPlan::Kafka(source) = plan.source else {
            panic!("Kafka inputs must produce a Kafka plan");
        };
        let offset = source
            .offset_state_placement
            .expect("domain offsets require a state placement");

        assert_eq!(offset.primary_node, Some(named("node-a")));
        assert_eq!(offset.placement.domain, named("sales"));
        assert_eq!(offset.placement.state, RuntimeState::KafkaOffset);
        assert_eq!(
            offset.placement.identifier,
            ModelName::from(&plan.ingestor.name)
        );
    }

    #[test]
    fn answers_quiesce_support_from_the_source_vocabulary() {
        let case = mqtt_case();
        let scheduled = scheduled_ingestor(case.source);
        let plan = IngestorStartPlan::decide(&named("sales"), &scheduled, &case.source_model)
            .expect("matching scheduled inputs must produce a plan");

        // A clean, at-most-once MQTT session has nothing the broker would hold for it, so the
        // vocabulary declares that it cannot suspend while it may still drop.
        assert!(!plan.ingestor.quiesce.supports(&IngestQuiesceMode::Suspend));
        assert!(plan.ingestor.quiesce.supports(&IngestQuiesceMode::Drop));
    }

    #[test]
    fn rejects_a_resolved_client_with_the_wrong_identity() {
        let SourceCase {
            source,
            mut source_model,
            ..
        } = http_case();
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

    #[test]
    fn rejects_a_resolved_source_of_another_kind() {
        let source = http_case().source;
        let source_model = kafka_case().source_model;

        let error =
            IngestorStartPlan::decide(&named("sales"), &scheduled_ingestor(source), &source_model)
                .expect_err("a Kafka client cannot serve an HTTP source");

        assert_eq!(
            error.current_context(),
            &IngestorStartPlanError::SourceKindMismatch
        );
    }
}
