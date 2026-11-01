//! Decides the complete specification for starting one scheduled ingestor.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an ingestor and the input it reads into one typed start plan, validating
//!   its source name, source kind, client, codec and route identities together, and deciding a
//!   client source's acknowledgement window, timeouts and endpoint contract.
//! - **Depends on.** Validated schedule Models, the entrypoint route planner and vocabulary
//!   values.
//! - **Must not know.** Tokio, locks, shared maps, connector I/O, node-local resources, or task
//!   spawning.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    AckWindow, ClientConfigEntry, ClientEndpointContract, ClientName, ClientProducerIngestor,
    ClientProducerPolicy, ClientResourceMount, ClusterNodeName, CodecName, CreateClientWebsockets,
    CreateIngestor, CreateSchema, DomainName, EndpointIngestMode, EndpointName, FlushPolicy,
    GeneralErrorPolicy, HttpEndpointIngestor, HttpIngestor, HttpRequestIngestor,
    IngestAcknowledgement, IngestQuiesceMode, IngestTimestampSource, IngestorEntity,
    IngestorEntityKind, IngestorInput, IngestorName, KafkaIngestMode, KafkaIngestor,
    KafkaOffsetMode, KafkaTopicIngestor, MessageErrorOperation, Model, ModelName, MqttIngestMode,
    MqttIngestor, MqttTopicIngestor, NatsIngestor, NatsSubjectIngestor, PrometheusIngestor,
    PulsarIngestMode, PulsarIngestor, PulsarTopicIngestor, RabbitMqIngestMode, RabbitMqIngestor,
    RabbitMqQueueIngestor, RedisIngestor, RedisPubSubIngestor, ResolvedBranching, ScheduledNode,
    SchemaField, SchemaName, SignalingProtocolName, SqsIngestMode, SqsIngestor, SqsQueueIngestor,
    SyslogIngestor, SyslogListenerIngestor, WebsocketsConnectionIngestor,
    WebsocketsEndpointIngestor, WebsocketsIngestor, ZeroMqIngestor, ZeroMqPullIngestor,
    parse_duration_text,
};
use nervix_primitives::sync::Arc;

use super::{
    domain_activation_plan::DomainActivationPlan,
    entrypoint_plan::{
        EntrypointOwner, EntrypointPlanError, EntrypointRouteContext, LoweredFilter,
        PlannedEntryRoute,
    },
};
use crate::runtime_schema::CompiledSchema;

/// The input an ingestor declares, asked for its entity kind and the quiesce modes it honors.
///
/// Both come from the entity vocabulary, which is the one declaration of what an input carries
/// and which quiesce modes it supports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeclaredIngestorInput {
    input: IngestorInput,
}

impl DeclaredIngestorInput {
    /// The entity kind of the input, which decides whether its messages carry headers and
    /// which metadata they expose to the ingestor's programs.
    pub(crate) fn entity_kind(&self) -> IngestorEntityKind {
        self.input.entity_kind()
    }

    pub(crate) fn quiesce_mode(&self) -> &IngestQuiesceMode {
        self.input.quiesce()
    }

    pub(crate) fn supports_quiesce(&self, mode: &IngestQuiesceMode) -> bool {
        self.input.supports_quiesce(mode)
    }
}

/// The ingestor itself: what it filters and routes, independent of the input it reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IngestorSpec {
    pub(crate) domain: DomainName,
    pub(crate) name: IngestorName,
    /// Every output route in declared order, which is never empty.
    pub(crate) routes: Vec<PlannedEntryRoute>,
    pub(crate) timestamp_source: Option<IngestTimestampSource>,
    pub(crate) filter_where: Option<LoweredFilter>,
    pub(crate) declared_input: DeclaredIngestorInput,
    /// The ingestor's node-wide `ON GENERAL ERROR` policy, which also reports the payloads its
    /// codec refuses.
    pub(crate) general_error_policy: GeneralErrorPolicy,
}

impl IngestorSpec {
    /// Whether the ingestor's programs may read the transport headers of its messages.
    pub(crate) fn reads_headers(&self) -> bool {
        self.declared_input.entity_kind().reads_headers()
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
            return Err(Report::new(EntrypointPlanError::InputIdentityMismatch {
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

client_plan!(HttpRequestIngestorStartPlan {
    method: String,
    headers: Vec<nervix_models::HttpRequestHeader>,
    max_body_size: nervix_models::PayloadSizeLimit,
    every: nervix_models::DomainClockPeriod,
});

/// Where a Kafka ingestor keeps the offsets it resumes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KafkaOffsetPlan {
    /// The broker keeps them for this consumer group.
    ConsumerGroup(nervix_models::KafkaConsumerGroupName),
    /// The domain keeps them as node-owned state, originated by the scheduled primary.
    Domain(KafkaDomainOffsetPlacement),
}

impl KafkaOffsetPlan {
    fn decide(mode: &KafkaOffsetMode, primary_node: Option<&ClusterNodeName>) -> Self {
        match mode {
            KafkaOffsetMode::ConsumerGroup(group) => Self::ConsumerGroup(group.clone()),
            KafkaOffsetMode::Domain => Self::Domain(KafkaDomainOffsetPlacement {
                primary_node: primary_node.cloned(),
            }),
        }
    }
}

/// The cluster node that originates a Kafka ingestor's domain offsets. An ingestor planned without
/// a placement, as a graph is before the cluster schedules it, has no originating node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KafkaDomainOffsetPlacement {
    pub(crate) primary_node: Option<ClusterNodeName>,
}

client_plan!(KafkaTopicIngestorStartPlan {
    topic: nervix_models::KafkaTopicName,
    offsets: KafkaOffsetPlan,
    instances: NonZeroU64,
    mode: KafkaIngestMode,
});
client_plan!(PulsarTopicIngestorStartPlan {
    topic: nervix_models::PulsarTopicName,
    subscription: nervix_models::PulsarSubscription,
    instances: NonZeroU64,
    mode: PulsarIngestMode,
});
client_plan!(MqttTopicIngestorStartPlan {
    topic: nervix_models::MqttTopicFilter,
    instances: NonZeroU64,
    session: nervix_models::MqttSession,
    qos: nervix_models::MqttQos,
    mode: MqttIngestMode,
});
client_plan!(NatsSubjectIngestorStartPlan {
    subject: nervix_models::NatsSubjectName,
    queue_group: nervix_models::NatsQueueGroupName,
    instances: NonZeroU64,
    mode: nervix_models::NatsIngestMode,
});
client_plan!(RabbitMqQueueIngestorStartPlan {
    queue: nervix_models::RabbitMqQueueName,
    instances: NonZeroU64,
    mode: RabbitMqIngestMode,
});
client_plan!(RedisPubSubIngestorStartPlan {
    channel: nervix_models::RedisChannelName,
    mode: nervix_models::RedisPubSubIngestMode,
});
client_plan!(PrometheusQueryIngestorStartPlan {
    query: String,
    max_body_size: nervix_models::PayloadSizeLimit,
    every: nervix_models::DomainClockPeriod,
});
client_plan!(ZeroMqPullIngestorStartPlan {
    mode: nervix_models::ZeroMqIngestMode,
});
client_plan!(SqsQueueIngestorStartPlan {
    queue: nervix_models::SqsQueueName,
    instances: NonZeroU64,
    mode: SqsIngestMode,
});
client_plan!(WebsocketsConnectionIngestorStartPlan {
    headers: nervix_models::WebsocketsUpgradeHeaders,
    mode: nervix_models::WebsocketsIngestMode,
    signaling_protocol: Option<SignalingProtocolName>,
});
client_plan!(SyslogListenerIngestorStartPlan {});

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndpointIntakeStartPlan {
    pub(crate) endpoint: EndpointName,
    pub(crate) mode: EndpointRequestPlan,
}

impl EndpointIntakeStartPlan {
    fn decide(
        ingestor: &IngestorName,
        expected: &EndpointName,
        resolved: &nervix_models::CreateEndpoint,
        mode: &EndpointIngestMode,
        quiesce: &IngestQuiesceMode,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        if expected != &resolved.name {
            return Err(Report::new(EntrypointPlanError::InputIdentityMismatch {
                ingestor: ingestor.clone(),
                expected: ModelName::from(expected),
                resolved: ModelName::from(&resolved.name),
            }));
        }
        if matches!(mode, EndpointIngestMode::Ack { .. })
            && resolved.endpoint_type == nervix_models::EndpointType::Websockets
            && resolved.signaling_protocol.is_none()
        {
            return Err(Report::new(
                EntrypointPlanError::MissingEndpointAckProtocol {
                    ingestor: ingestor.clone(),
                    endpoint: resolved.name.clone(),
                },
            ));
        }
        Ok(Self {
            endpoint: resolved.name.clone(),
            mode: EndpointRequestPlan::decide(ingestor, mode, quiesce)?,
        })
    }
}

/// A request policy already checked and converted at the decision boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EndpointRequestPlan {
    NoAck,
    Ack {
        max: NonZeroUsize,
        timeout: Duration,
        retry_after: Duration,
    },
}

impl EndpointRequestPlan {
    fn decide(
        ingestor: &IngestorName,
        mode: &EndpointIngestMode,
        quiesce: &IngestQuiesceMode,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        match mode {
            EndpointIngestMode::NoAck => Ok(Self::NoAck),
            EndpointIngestMode::Ack { max, timeout } => {
                let invalid = EntrypointPlanError::InvalidEndpointDuration {
                    ingestor: ingestor.clone(),
                    clause: "ACK TIMEOUT",
                    value: timeout.clone(),
                };
                let timeout =
                    parse_duration_text(timeout).change_context_lazy(|| invalid.clone())?;
                if timeout.is_zero() || timeout > nervix_models::ClientDeadline::LONGEST {
                    return Err(Report::new(invalid));
                }
                let IngestQuiesceMode::Reject { retry_after } = quiesce else {
                    return Err(Report::new(
                        EntrypointPlanError::UnsupportedEndpointAckQuiesce {
                            ingestor: ingestor.clone(),
                            quiesce: quiesce.kind(),
                        },
                    ));
                };
                let retry_after = parse_duration_text(retry_after).change_context_lazy(|| {
                    EntrypointPlanError::InvalidEndpointDuration {
                        ingestor: ingestor.clone(),
                        clause: "ON QUIESCE REJECT RETRY AFTER",
                        value: retry_after.clone(),
                    }
                })?;
                if retry_after > nervix_models::ClientDeadline::LONGEST {
                    return Err(Report::new(EntrypointPlanError::InvalidEndpointDuration {
                        ingestor: ingestor.clone(),
                        clause: "ON QUIESCE REJECT RETRY AFTER",
                        value: humantime::format_duration(retry_after).to_string(),
                    }));
                }
                let max = NonZeroUsize::new(usize::from(max.get()))
                    .assured("a positive u16 fits every supported native usize");
                Ok(Self::Ack {
                    max,
                    timeout,
                    retry_after,
                })
            }
        }
    }
}

/// Everything the host needs to start one ingestor: the ingestor itself and the plan of the input
/// it reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IngestorStartPlan {
    pub(crate) ingestor: IngestorSpec,
    pub(crate) input: IngestorInputPlan,
}

/// The plan of what one ingestor reads. A transport's payloads are decoded by its codec; a client
/// source's batches already carry its input schema, so it names none.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IngestorInputPlan {
    Transport(TransportInputPlan),
    Client(ClientIngestorStartPlan),
}

/// A transport source and the codec that decodes its payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportInputPlan {
    pub(crate) codec: CodecName,
    pub(crate) source: IngestorConnectorPlan,
}

/// A client source: the schema every submitted batch matches, the policy its producers submit
/// under, and the contract they attach to.
#[derive(Debug, Clone)]
pub(crate) struct ClientIngestorStartPlan {
    pub(crate) schema_name: SchemaName,
    /// The declared fields in order, with their optionality and sensitivity, as producers are told
    /// them.
    pub(crate) fields: Vec<SchemaField>,
    pub(crate) schema: Arc<CompiledSchema>,
    pub(crate) policy: ClientProducerPolicy,
    pub(crate) contract: ClientEndpointContract,
}

impl ClientIngestorStartPlan {
    /// How many submitted batches may await their acknowledgement across every producer.
    pub(crate) fn window_size(&self) -> NonZeroUsize {
        let declared = match self.policy.window {
            AckWindow::Sequential => NonZeroU64::MIN,
            AckWindow::Parallel { max } => max,
        };
        NonZeroUsize::new(declared.get().arch_into())
            .assured("a non-zero declared window is still non-zero at this target's pointer width")
    }
}

/// Plans compare by what they decide; the compiled schema is derived from `fields`.
impl PartialEq for ClientIngestorStartPlan {
    fn eq(&self, other: &Self) -> bool {
        self.schema_name == other.schema_name
            && self.fields == other.fields
            && self.policy == other.policy
            && self.contract == other.contract
    }
}

/// The plan of one ingestor's source, which the composition root maps to the connector that runs
/// it.
/// The topic entity planned for Kafka execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KafkaIngestorStartPlan {
    Topic(KafkaTopicIngestorStartPlan),
}

/// The topic entity planned for Pulsar execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PulsarIngestorStartPlan {
    Topic(PulsarTopicIngestorStartPlan),
}

/// The queue entity planned for RabbitMq execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RabbitMqIngestorStartPlan {
    Queue(RabbitMqQueueIngestorStartPlan),
}

/// The pubsub entity planned for Redis execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RedisIngestorStartPlan {
    PubSub(RedisPubSubIngestorStartPlan),
}

/// The topic entity planned for Mqtt execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MqttIngestorStartPlan {
    Topic(MqttTopicIngestorStartPlan),
}

/// The subject entity planned for Nats execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NatsIngestorStartPlan {
    Subject(NatsSubjectIngestorStartPlan),
}

/// The pull entity planned for ZeroMq execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ZeroMqIngestorStartPlan {
    Pull(ZeroMqPullIngestorStartPlan),
}

/// The queue entity planned for Sqs execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqsIngestorStartPlan {
    Queue(SqsQueueIngestorStartPlan),
}

/// The query entity planned for Prometheus execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PrometheusIngestorStartPlan {
    Query(PrometheusQueryIngestorStartPlan),
}

/// The listener entity planned for Syslog execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SyslogIngestorStartPlan {
    Listener(SyslogListenerIngestorStartPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpEndpointIngestorStartPlan(pub(crate) EndpointIntakeStartPlan);
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WebsocketsEndpointIngestorStartPlan(pub(crate) EndpointIntakeStartPlan);
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HttpIngestorStartPlan {
    Request(HttpRequestIngestorStartPlan),
    Endpoint(HttpEndpointIngestorStartPlan),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WebsocketsIngestorStartPlan {
    Connection(WebsocketsConnectionIngestorStartPlan),
    Endpoint(WebsocketsEndpointIngestorStartPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IngestorConnectorPlan {
    Http(HttpIngestorStartPlan),
    Kafka(KafkaIngestorStartPlan),
    Pulsar(PulsarIngestorStartPlan),
    Mqtt(MqttIngestorStartPlan),
    Nats(NatsIngestorStartPlan),
    RabbitMq(RabbitMqIngestorStartPlan),
    Redis(RedisIngestorStartPlan),
    Prometheus(PrometheusIngestorStartPlan),
    ZeroMq(ZeroMqIngestorStartPlan),
    Sqs(SqsIngestorStartPlan),
    Websockets(WebsocketsIngestorStartPlan),
    Syslog(SyslogIngestorStartPlan),
}

impl IngestorStartPlan {
    /// Whether replacing this input retires external state that outlives its execution.
    /// Routes and codecs do not identify the MQTT session, so changing them alone retains it.
    /// A changed owned Pulsar source needs a resolved subscription comparison at composition;
    /// an absent replacement removes the owned subscription after its source tasks stop.
    pub(crate) fn requires_entity_retirement(&self, replacement: Option<&Self>) -> bool {
        let IngestorInputPlan::Transport(input) = &self.input else {
            return false;
        };
        match &input.source {
            IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) => {
                if let Some(replacement) = replacement
                    && let IngestorInputPlan::Transport(input) = &replacement.input
                    && let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(replacement)) =
                        &input.source
                {
                    return source != replacement;
                }
                true
            }
            IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) => {
                let owned = source.subscription.ownership.is_owned();
                if !owned {
                    return false;
                }
                if let Some(replacement) = replacement
                    && let IngestorInputPlan::Transport(input) = &replacement.input
                    && let IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(
                        replacement,
                    )) = &input.source
                {
                    return source != replacement;
                }
                true
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(_))
            | IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(_))
            | IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(_))
            | IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(_))
            | IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(_))
            | IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(_))
            | IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(_))
            | IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Endpoint(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(_))
            | IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(_)) => false,
        }
    }

    /// Decides the start plan of one scheduled ingestor from the node its input resolved to: the
    /// client or endpoint a transport reads through, or the schema a client source's batches
    /// carry.
    pub(in crate::registry) fn decide(
        domain: &DomainName,
        scheduled: &ScheduledNode,
        ingestor: &CreateIngestor,
        source_model: &Model,
        routes: &EntrypointRouteContext<'_>,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let DecidedInput {
            plan: input,
            schema: input_schema,
        } = IngestorInputPlan::decide(ingestor, source_model, scheduled, routes.activation())?;
        let identifier = ModelName::from(&ingestor.name);
        let owner = EntrypointOwner::ingestor(&identifier, ingestor);
        let input_arrow_schema = input_schema.arrow_schema();
        let routes = routes.plan_routes(
            &owner,
            ingestor.output_routes.outputs(),
            &input_arrow_schema,
        )?;
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
            timestamp_source: ingestor.timestamp_source.clone(),
            filter_where,
            declared_input: DeclaredIngestorInput {
                input: ingestor.input.clone(),
            },
            general_error_policy: ingestor.general_error_policy.clone(),
        };
        Ok(Self { ingestor, input })
    }

    /// The acknowledgement a transport's delivery mode declares. A transport whose statement
    /// declares no delivery mode acknowledges nothing. A client source acknowledges through its
    /// own window and has no transport acknowledgement.
    pub(crate) fn transport_acknowledgement(&self) -> Option<IngestAcknowledgement<'_>> {
        let IngestorInputPlan::Transport(transport) = &self.input else {
            return None;
        };
        let acknowledgement = match &transport.source {
            IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(plan)) => {
                plan.mode.acknowledgement()
            }
            // Endpoint windows and durations are already typed and belong to request intake,
            // which never runs the broker acknowledgement loop.
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(_))
            | IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Endpoint(_)) => {
                return None;
            }
            IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(plan)) => {
                plan.mode.acknowledgement()
            }
            IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(_))
            | IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(_))
            | IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(_)) => {
                IngestAcknowledgement::Unacknowledged
            }
        };
        Some(acknowledgement)
    }

    /// Whether this ingestor keeps the offsets it resumes from as domain state, which its
    /// placement then replicates.
    pub(crate) fn keeps_domain_offsets(&self) -> bool {
        matches!(
            &self.input,
            IngestorInputPlan::Transport(TransportInputPlan {
                source: IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(
                    KafkaTopicIngestorStartPlan {
                        offsets: KafkaOffsetPlan::Domain(_),
                        ..
                    }
                )),
                ..
            })
        )
    }
}

/// An ingestor's input plan together with the compiled schema its programs read.
struct DecidedInput {
    plan: IngestorInputPlan,
    schema: Arc<CompiledSchema>,
}

impl IngestorInputPlan {
    fn decide(
        ingestor: &CreateIngestor,
        source_model: &Model,
        scheduled: &ScheduledNode,
        activation: &DomainActivationPlan,
    ) -> Result<DecidedInput, Report<EntrypointPlanError>> {
        match &ingestor.input {
            IngestorInput::Transport(transport) => {
                let source = IngestorConnectorPlan::decide(
                    &ingestor.name,
                    &transport.source,
                    source_model,
                    scheduled,
                )?;
                let Some(codec) = activation.codecs.get(&transport.codec) else {
                    return Err(Report::new(EntrypointPlanError::MissingCodec {
                        ingestor: ingestor.name.clone(),
                        codec: transport.codec.clone(),
                    }));
                };
                Ok(DecidedInput {
                    plan: Self::Transport(TransportInputPlan {
                        codec: transport.codec.clone(),
                        source,
                    }),
                    schema: codec.schema.clone(),
                })
            }
            IngestorInput::Client(client) => {
                let plan =
                    ClientIngestorStartPlan::decide(ingestor, client, source_model, activation)?;
                let schema = plan.schema.clone();
                Ok(DecidedInput {
                    plan: Self::Client(plan),
                    schema,
                })
            }
        }
    }
}

impl ClientIngestorStartPlan {
    fn decide(
        ingestor: &CreateIngestor,
        client: &ClientProducerIngestor,
        source_model: &Model,
        activation: &DomainActivationPlan,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        let Model::Schema(schema_model) = source_model else {
            return Err(Report::new(EntrypointPlanError::InputKindMismatch {
                ingestor: ingestor.name.clone(),
                resolved: source_model.kind(),
            }));
        };
        if schema_model.name != client.schema {
            return Err(Report::new(EntrypointPlanError::InputIdentityMismatch {
                ingestor: ingestor.name.clone(),
                expected: ModelName::from(&client.schema),
                resolved: ModelName::from(&schema_model.name),
            }));
        }
        let Some(schema) = activation.schemas.get(&client.schema) else {
            return Err(Report::new(EntrypointPlanError::MissingClientSchema {
                ingestor: ingestor.name.clone(),
                schema: client.schema.clone(),
            }));
        };
        let name = &ingestor.name;
        let mode = &client.mode;
        let policy = ClientProducerPolicy {
            window: mode.window,
            ack_timeout: Self::duration(name, "ACK TIMEOUT", &mode.ack_timeout)?,
            retry_backoff: Self::duration(
                name,
                "RETRY POLICY BACKOFF",
                &mode.retry_policy.backoff,
            )?,
            retry_max_backoff: Self::duration(
                name,
                "RETRY POLICY MAX",
                &mode.retry_policy.max_backoff,
            )?,
        };
        let contract = client_endpoint_contract(ingestor, schema_model, activation)?;
        Ok(Self {
            schema_name: client.schema.clone(),
            fields: schema_model.fields.clone(),
            schema: schema.clone(),
            policy,
            contract,
        })
    }
}

impl ClientIngestorStartPlan {
    /// One policy duration of a client source. Validation admitted only positive durations, so a
    /// value that does not read as one here is a model the registry did not validate.
    fn duration(
        ingestor: &IngestorName,
        clause: &'static str,
        value: &str,
    ) -> Result<Duration, Report<EntrypointPlanError>> {
        let invalid = EntrypointPlanError::InvalidClientDuration {
            ingestor: ingestor.clone(),
            clause,
            value: value.to_string(),
        };
        let parsed = parse_duration_text(value).change_context_lazy(|| invalid.clone())?;
        if parsed.is_zero() {
            return Err(Report::new(invalid));
        }
        Ok(parsed)
    }
}

/// The identity of a client ingestor's endpoint contract: the ingestor as producers see it, its
/// input schema, and the branch declarations its routes construct. Flush cadence is left out, so
/// a change that only retunes flushing keeps every producer attached.
fn client_endpoint_contract(
    ingestor: &CreateIngestor,
    schema: &CreateSchema,
    activation: &DomainActivationPlan,
) -> Result<ClientEndpointContract, Report<EntrypointPlanError>> {
    let unrenderable = || EntrypointPlanError::UnrenderableClientContract {
        ingestor: ingestor.name.clone(),
    };
    let mut contract_ingestor = ingestor.clone();
    for route in &mut contract_ingestor.output_routes.routes {
        route.flush_policy = Some(FlushPolicy::Immediate);
    }
    let mut hasher = blake3::Hasher::new();
    let mut add = |part: &str| {
        let length: u64 = part.len().arch_into();
        hasher.update(&length.to_le_bytes());
        hasher.update(part.as_bytes());
    };
    add(&contract_ingestor
        .to_canonical_plexus()
        .change_context_lazy(unrenderable)?);
    add(&schema
        .to_canonical_plexus()
        .change_context_lazy(unrenderable)?);
    for route in ingestor.output_routes.outputs() {
        add(route.relay.as_str());
        let branching = activation
            .relays
            .get(&route.relay)
            .map(|relay| &relay.branching);
        match branching {
            Some(ResolvedBranching::Branched { branch, schema, .. }) => {
                add(branch.as_str());
                add(&schema
                    .to_canonical_plexus()
                    .change_context_lazy(unrenderable)?);
            }
            Some(ResolvedBranching::Unbranched) | None => add("UNBRANCHED"),
        }
    }
    Ok(ClientEndpointContract::from_digest(
        *hasher.finalize().as_bytes(),
    ))
}

impl WebsocketsConnectionIngestorStartPlan {
    fn decide(
        name: &IngestorName,
        source: &WebsocketsConnectionIngestor,
        client: &CreateClientWebsockets,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        Ok(Self {
            headers: nervix_models::WebsocketsUpgradeHeaders::parse(&source.headers)
                .change_context(EntrypointPlanError::InvalidWebsocketsUpgradeHeaders {
                    ingestor: name.clone(),
                })?,
            client: IngestorClientSpec::resolved(
                name,
                &source.client,
                &client.name,
                client.mount.as_ref(),
                &client.config,
            )?,
            mode: source.mode.clone(),
            signaling_protocol: client.signaling_protocol.clone(),
        })
    }
}

impl IngestorConnectorPlan {
    fn decide(
        name: &IngestorName,
        source: &IngestorEntity,
        source_model: &Model,
        scheduled: &ScheduledNode,
    ) -> Result<Self, Report<EntrypointPlanError>> {
        match (source, source_model) {
            (
                IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                    client: expected,
                    method,
                    headers,
                    max_body_size,
                    every,
                    ..
                })),
                Model::ClientHttp(resolved),
            ) => Ok(Self::Http(HttpIngestorStartPlan::Request(
                HttpRequestIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    method: method.clone(),
                    headers: headers.clone(),
                    max_body_size: max_body_size
                        .unwrap_or(nervix_models::HTTP_DEFAULT_MAX_BODY_SIZE),
                    every: *every,
                },
            ))),
            (
                IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                    client: expected,
                    topic,
                    offset_mode,
                    instances,
                    mode,
                    ..
                })),
                Model::ClientKafka(resolved),
            ) => {
                let offsets = KafkaOffsetPlan::decide(
                    offset_mode,
                    scheduled.single_unit().primary_node.as_ref(),
                );
                Ok(Self::Kafka(KafkaIngestorStartPlan::Topic(
                    KafkaTopicIngestorStartPlan {
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
                    },
                )))
            }
            (
                IngestorEntity::Pulsar(PulsarIngestor::Topic(PulsarTopicIngestor {
                    client: expected,
                    topic,
                    subscription,
                    instances,
                    mode,
                    ..
                })),
                Model::ClientPulsar(resolved),
            ) => Ok(Self::Pulsar(PulsarIngestorStartPlan::Topic(
                PulsarTopicIngestorStartPlan {
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
                },
            ))),
            (
                IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                    client: expected,
                    topic,
                    instances,
                    session,
                    qos,
                    mode,
                    ..
                })),
                Model::ClientMqtt(resolved),
            ) => Ok(Self::Mqtt(MqttIngestorStartPlan::Topic(
                MqttTopicIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    topic: topic.clone(),
                    instances: *instances,
                    session: *session,
                    qos: *qos,
                    mode: mode.clone(),
                },
            ))),
            (
                IngestorEntity::Nats(NatsIngestor::Subject(NatsSubjectIngestor {
                    client: expected,
                    subject,
                    queue_group,
                    instances,
                    mode,
                    ..
                })),
                Model::ClientNats(resolved),
            ) => Ok(Self::Nats(NatsIngestorStartPlan::Subject(
                NatsSubjectIngestorStartPlan {
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
                },
            ))),
            (
                IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(RabbitMqQueueIngestor {
                    client: expected,
                    queue,
                    instances,
                    mode,
                    ..
                })),
                Model::ClientRabbitMq(resolved),
            ) => Ok(Self::RabbitMq(RabbitMqIngestorStartPlan::Queue(
                RabbitMqQueueIngestorStartPlan {
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
                },
            ))),
            (
                IngestorEntity::Redis(RedisIngestor::PubSub(RedisPubSubIngestor {
                    client: expected,
                    channel,
                    mode,
                    ..
                })),
                Model::ClientRedis(resolved),
            ) => Ok(Self::Redis(RedisIngestorStartPlan::PubSub(
                RedisPubSubIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    channel: channel.clone(),
                    mode: mode.clone(),
                },
            ))),
            (
                IngestorEntity::Prometheus(PrometheusIngestor::Query(source)),
                Model::ClientPrometheus(resolved),
            ) => Ok(Self::Prometheus(PrometheusIngestorStartPlan::Query(
                PrometheusQueryIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        &source.client,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    query: source.query.clone(),
                    max_body_size: source.max_body_size_in_effect(),
                    every: source.every,
                },
            ))),
            (
                IngestorEntity::ZeroMq(ZeroMqIngestor::Pull(ZeroMqPullIngestor {
                    client: expected,
                    mode,
                    ..
                })),
                Model::ClientZeroMq(resolved),
            ) => Ok(Self::ZeroMq(ZeroMqIngestorStartPlan::Pull(
                ZeroMqPullIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                    mode: mode.clone(),
                },
            ))),
            (
                IngestorEntity::Sqs(SqsIngestor::Queue(SqsQueueIngestor {
                    client: expected,
                    queue,
                    instances,
                    mode,
                    ..
                })),
                Model::ClientSqs(resolved),
            ) => Ok(Self::Sqs(SqsIngestorStartPlan::Queue(
                SqsQueueIngestorStartPlan {
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
                },
            ))),
            (
                IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
                    endpoint: expected,
                    mode,
                    quiesce,
                    ..
                })),
                Model::Endpoint(resolved),
            ) => {
                let intake =
                    EndpointIntakeStartPlan::decide(name, expected, resolved, mode, quiesce)?;
                Ok(Self::Http(HttpIngestorStartPlan::Endpoint(
                    HttpEndpointIngestorStartPlan(intake),
                )))
            }
            (
                IngestorEntity::Websockets(WebsocketsIngestor::Endpoint(
                    WebsocketsEndpointIngestor {
                        endpoint: expected,
                        mode,
                        quiesce,
                        ..
                    },
                )),
                Model::Endpoint(resolved),
            ) => {
                let intake =
                    EndpointIntakeStartPlan::decide(name, expected, resolved, mode, quiesce)?;
                Ok(Self::Websockets(WebsocketsIngestorStartPlan::Endpoint(
                    WebsocketsEndpointIngestorStartPlan(intake),
                )))
            }
            (
                IngestorEntity::Websockets(WebsocketsIngestor::Connection(source)),
                Model::ClientWebsockets(resolved),
            ) => Ok(Self::Websockets(WebsocketsIngestorStartPlan::Connection(
                WebsocketsConnectionIngestorStartPlan::decide(name, source, resolved)?,
            ))),
            (
                IngestorEntity::Syslog(SyslogIngestor::Listener(SyslogListenerIngestor {
                    client: expected,
                    ..
                })),
                Model::ClientSyslog(resolved),
            ) => Ok(Self::Syslog(SyslogIngestorStartPlan::Listener(
                SyslogListenerIngestorStartPlan {
                    client: IngestorClientSpec::resolved(
                        name,
                        expected,
                        &resolved.name,
                        resolved.mount.as_ref(),
                        &resolved.config,
                    )?,
                },
            ))),
            _ => Err(Report::new(EntrypointPlanError::InputKindMismatch {
                ingestor: name.clone(),
                resolved: source_model.kind(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        ClientPoolBounds, CreateClientHttp, CreateClientKafka, CreateClientMqtt, CreateClientNats,
        CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
        CreateClientSqs, CreateClientSyslog, CreateClientWebsockets, CreateClientZeroMq,
        CreateEndpoint, EndpointType, FlushPolicy, GeneralErrorPolicy, IngestorName, MqttQos,
        MqttSession, NodeRef, OutputBranch, ProcessorOutputs, PrometheusQueryIngestor,
        PulsarSubscriptionOwnership,
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

    fn ingestor_model(source: IngestorEntity) -> Model {
        Model::Ingestor(CreateIngestor {
            name: named("source"),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("events")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            input: nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
                source,
                codec: named("json"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        })
    }

    /// The domain one ingestor reads from `source_model` in: its schema, codec, relay and the
    /// VHOST an endpoint source publishes on.
    fn domain_models(source: IngestorEntity, source_model: Model) -> Vec<Model> {
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

    /// The transport input of a fixture ingestor, which always reads a transport.
    fn transport_plan(plan: &IngestorStartPlan) -> &TransportInputPlan {
        let IngestorInputPlan::Transport(transport) = &plan.input else {
            panic!(
                "the fixture ingestor reads a transport, planned as {:?}",
                plan.input
            );
        };
        transport
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

    /// The subscription the Pulsar fixture reads, which its ingestor owns.
    fn pulsar_subscription() -> nervix_models::PulsarSubscription {
        nervix_models::PulsarSubscription {
            name: named("nervix"),
            ownership: PulsarSubscriptionOwnership::Ingestor {
                start_at: nervix_models::PulsarStartPosition::Earliest,
            },
        }
    }

    /// One source statement, the model it resolves to, and the source plan and header access they
    /// decide.
    struct IngestorCase {
        source: IngestorEntity,
        source_model: Model,
        plan: IngestorConnectorPlan,
        reads_headers: bool,
    }

    fn http_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Http(HttpIngestor::Request(HttpRequestIngestor {
                client: named("upstream"),
                method: "GET".to_owned(),
                headers: Vec::new(),
                max_body_size: None,
                every: cadence(),
                mode: nervix_models::HttpRequestIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientHttp(CreateClientHttp {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Http(HttpIngestorStartPlan::Request(
                HttpRequestIngestorStartPlan {
                    client: client(),
                    method: "GET".to_owned(),
                    headers: Vec::new(),
                    max_body_size: nervix_models::HTTP_DEFAULT_MAX_BODY_SIZE,
                    every: cadence(),
                },
            )),
            reads_headers: true,
        }
    }

    fn kafka_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                client: named("upstream"),
                topic: named("events"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(named("nervix")),
                instances: nonzero!(1u64),
                mode: KafkaIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientKafka(CreateClientKafka {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(
                KafkaTopicIngestorStartPlan {
                    client: client(),
                    topic: named("events"),
                    offsets: KafkaOffsetPlan::ConsumerGroup(named("nervix")),
                    instances: nonzero!(1u64),
                    mode: KafkaIngestMode::NoAck,
                },
            )),
            reads_headers: true,
        }
    }

    fn pulsar_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Pulsar(PulsarIngestor::Topic(PulsarTopicIngestor {
                client: named("upstream"),
                topic: named("events"),
                subscription: pulsar_subscription(),
                instances: nonzero!(1u64),
                mode: PulsarIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientPulsar(CreateClientPulsar {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(
                PulsarTopicIngestorStartPlan {
                    client: client(),
                    topic: named("events"),
                    subscription: pulsar_subscription(),
                    instances: nonzero!(1u64),
                    mode: PulsarIngestMode::NoAck,
                },
            )),
            reads_headers: true,
        }
    }

    fn mqtt_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Mqtt(MqttIngestor::Topic(MqttTopicIngestor {
                client: named("upstream"),
                topic: nervix_models::MqttTopicFilter::parse("events/#")
                    .expect("valid topic filter"),
                instances: nonzero!(1u64),
                session: MqttSession::Clean,
                qos: MqttQos::Qos0,
                mode: MqttIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            })),
            source_model: Model::ClientMqtt(CreateClientMqtt {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(
                MqttTopicIngestorStartPlan {
                    client: client(),
                    topic: nervix_models::MqttTopicFilter::parse("events/#")
                        .expect("valid topic filter"),
                    instances: nonzero!(1u64),
                    session: MqttSession::Clean,
                    qos: MqttQos::Qos0,
                    mode: MqttIngestMode::NoAck,
                },
            )),
            reads_headers: false,
        }
    }

    fn nats_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Nats(NatsIngestor::Subject(NatsSubjectIngestor {
                client: named("upstream"),
                subject: named("events"),
                queue_group: named("nervix"),
                instances: nonzero!(1u64),
                mode: nervix_models::NatsIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            })),
            source_model: Model::ClientNats(CreateClientNats {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Nats(NatsIngestorStartPlan::Subject(
                NatsSubjectIngestorStartPlan {
                    client: client(),
                    subject: named("events"),
                    queue_group: named("nervix"),
                    instances: nonzero!(1u64),
                    mode: nervix_models::NatsIngestMode::NoAck,
                },
            )),
            reads_headers: true,
        }
    }

    fn rabbitmq_mode() -> RabbitMqIngestMode {
        RabbitMqIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: retry_policy(),
        }
    }

    fn rabbitmq_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(RabbitMqQueueIngestor {
                client: named("upstream"),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: rabbitmq_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientRabbitMq(CreateClientRabbitMq {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::RabbitMq(RabbitMqIngestorStartPlan::Queue(
                RabbitMqQueueIngestorStartPlan {
                    client: client(),
                    queue: named("events"),
                    instances: nonzero!(1u64),
                    mode: rabbitmq_mode(),
                },
            )),
            reads_headers: true,
        }
    }

    fn redis_pubsub_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Redis(RedisIngestor::PubSub(RedisPubSubIngestor {
                client: named("upstream"),
                channel: named("events"),
                mode: nervix_models::RedisPubSubIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Drop,
            })),
            source_model: Model::ClientRedis(CreateClientRedis {
                name: named("upstream"),
                pool: pool_bounds(),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Redis(RedisIngestorStartPlan::PubSub(
                RedisPubSubIngestorStartPlan {
                    client: client(),
                    channel: named("events"),
                    mode: nervix_models::RedisPubSubIngestMode::NoAck,
                },
            )),
            reads_headers: false,
        }
    }

    fn prometheus_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Prometheus(PrometheusIngestor::Query(
                PrometheusQueryIngestor {
                    client: named("upstream"),
                    query: "up".to_string(),
                    max_body_size: None,
                    every: cadence(),
                    mode: nervix_models::PrometheusIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                },
            )),
            source_model: Model::ClientPrometheus(CreateClientPrometheus {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Prometheus(PrometheusIngestorStartPlan::Query(
                PrometheusQueryIngestorStartPlan {
                    client: client(),
                    query: "up".to_string(),
                    max_body_size: nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
                    every: cadence(),
                },
            )),
            reads_headers: false,
        }
    }

    fn zeromq_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::ZeroMq(ZeroMqIngestor::Pull(ZeroMqPullIngestor {
                client: named("upstream"),
                mode: nervix_models::ZeroMqIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientZeroMq(CreateClientZeroMq {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::ZeroMq(ZeroMqIngestorStartPlan::Pull(
                ZeroMqPullIngestorStartPlan {
                    client: client(),
                    mode: nervix_models::ZeroMqIngestMode::NoAck,
                },
            )),
            reads_headers: false,
        }
    }

    fn sqs_mode() -> SqsIngestMode {
        SqsIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: retry_policy(),
        }
    }

    fn sqs_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Sqs(SqsIngestor::Queue(SqsQueueIngestor {
                client: named("upstream"),
                queue: named("events"),
                instances: nonzero!(1u64),
                mode: sqs_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientSqs(CreateClientSqs {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Sqs(SqsIngestorStartPlan::Queue(
                SqsQueueIngestorStartPlan {
                    client: client(),
                    queue: named("events"),
                    instances: nonzero!(1u64),
                    mode: sqs_mode(),
                },
            )),
            reads_headers: true,
        }
    }

    fn endpoint_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
                endpoint: named("upstream"),
                mode: nervix_models::EndpointIngestMode::NoAck,
                quiesce: IngestQuiesceMode::EndpointBuffer {
                    max_size: "1MiB".to_string(),
                },
            })),
            source_model: Model::Endpoint(CreateEndpoint {
                name: named("upstream"),
                on_vhost: named("public"),
                path: "/events".to_string(),
                endpoint_type: EndpointType::Http,
                signaling_protocol: None,
            }),
            plan: IngestorConnectorPlan::Http(HttpIngestorStartPlan::Endpoint(
                HttpEndpointIngestorStartPlan(EndpointIntakeStartPlan {
                    endpoint: named("upstream"),
                    mode: EndpointRequestPlan::NoAck,
                }),
            )),
            reads_headers: true,
        }
    }

    fn websockets_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Websockets(WebsocketsIngestor::Connection(
                WebsocketsConnectionIngestor {
                    client: named("upstream"),
                    headers: Vec::new(),
                    mode: nervix_models::WebsocketsIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Drop,
                },
            )),
            source_model: Model::ClientWebsockets(CreateClientWebsockets {
                name: named("upstream"),
                mount: None,
                signaling_protocol: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Websockets(WebsocketsIngestorStartPlan::Connection(
                WebsocketsConnectionIngestorStartPlan {
                    headers: nervix_models::WebsocketsUpgradeHeaders::default(),
                    client: client(),
                    mode: nervix_models::WebsocketsIngestMode::NoAck,
                    signaling_protocol: None,
                },
            )),
            reads_headers: true,
        }
    }

    fn syslog_case() -> IngestorCase {
        IngestorCase {
            source: IngestorEntity::Syslog(SyslogIngestor::Listener(SyslogListenerIngestor {
                client: named("upstream"),
                mode: nervix_models::SyslogIngestMode::NoAck,
                quiesce: IngestQuiesceMode::Suspend,
            })),
            source_model: Model::ClientSyslog(CreateClientSyslog {
                name: named("upstream"),
                mount: None,
                config: Vec::new(),
            }),
            plan: IngestorConnectorPlan::Syslog(SyslogIngestorStartPlan::Listener(
                SyslogListenerIngestorStartPlan { client: client() },
            )),
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
    fn decides_the_source_plan_of_each_source_kind(#[case] case: IngestorCase) {
        let IngestorCase {
            source,
            source_model,
            plan: expected_plan,
            reads_headers,
        } = case;
        let declared = source.clone();

        let plans = planned_entrypoints(domain_models(source, source_model))
            .assured("a matching source and ingestor produce a plan");
        let plan = start_plan(&plans);

        let transport = transport_plan(plan);
        assert_eq!(transport.source, expected_plan);
        assert_eq!(transport.codec, named("json"));
        assert_eq!(plan.ingestor.name, named("source"));
        assert_eq!(plan.ingestor.routes[0].relay, named("events"));
        assert_eq!(plan.ingestor.declared_input.entity_kind(), declared.kind());
        assert_eq!(plan.ingestor.reads_headers(), reads_headers);
        assert_eq!(
            plan.ingestor.declared_input.quiesce_mode(),
            declared.quiesce()
        );
        assert!(!plan.keeps_domain_offsets());
    }

    #[test]
    fn endpoint_ack_plan_converts_its_declared_window_and_durations_once() {
        for max in [1, 8, u16::MAX] {
            let mode = EndpointIngestMode::Ack {
                max: std::num::NonZeroU16::new(max).assured("the declared boundaries are positive"),
                timeout: "250ms".to_owned(),
            };
            let plan = EndpointRequestPlan::decide(
                &named("source"),
                &mode,
                &IngestQuiesceMode::Reject {
                    retry_after: "1500ms".to_owned(),
                },
            )
            .assured("a bounded acknowledged endpoint has a request plan");
            assert_eq!(
                plan,
                EndpointRequestPlan::Ack {
                    max: NonZeroUsize::new(usize::from(max)).assured("a positive u16 fits usize"),
                    timeout: Duration::from_millis(250),
                    retry_after: Duration::from_millis(1500),
                }
            );
        }
    }

    #[rstest]
    #[case("0s")]
    #[case("not-a-duration")]
    #[case("1000000years")]
    fn endpoint_ack_plan_refuses_an_unusable_deadline(#[case] timeout: &str) {
        let mode = EndpointIngestMode::Ack {
            max: nonzero!(8u16),
            timeout: timeout.to_owned(),
        };
        let error = EndpointRequestPlan::decide(
            &named("source"),
            &mode,
            &IngestQuiesceMode::Reject {
                retry_after: "1s".to_owned(),
            },
        )
        .expect_err("the deadline must be positive and representable by the physical timer");
        assert!(
            matches!(error.current_context(), EntrypointPlanError::InvalidEndpointDuration {
            ingestor, clause: "ACK TIMEOUT", ..
        } if ingestor == &named::<IngestorName>("source"))
        );
    }

    #[test]
    fn endpoint_ack_plan_refuses_quiesce_buffering_at_the_model_boundary() {
        let mode = EndpointIngestMode::Ack {
            max: nonzero!(8u16),
            timeout: "5s".to_owned(),
        };
        let error = EndpointRequestPlan::decide(
            &named("source"),
            &mode,
            &IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_owned(),
            },
        )
        .expect_err("request acknowledgement requires sender-owned retries");
        assert!(matches!(
            error.current_context(),
            EntrypointPlanError::UnsupportedEndpointAckQuiesce { .. }
        ));
    }

    #[test]
    fn an_acknowledged_websocket_endpoint_requires_its_declared_wire_protocol() {
        let source =
            IngestorEntity::Websockets(WebsocketsIngestor::Endpoint(WebsocketsEndpointIngestor {
                endpoint: named("upstream"),
                mode: EndpointIngestMode::Ack {
                    max: nonzero!(8u16),
                    timeout: "5s".to_owned(),
                },
                quiesce: IngestQuiesceMode::Reject {
                    retry_after: "1s".to_owned(),
                },
            }));
        let endpoint = Model::Endpoint(CreateEndpoint {
            name: named("upstream"),
            on_vhost: named("public"),
            path: "/events".to_owned(),
            endpoint_type: EndpointType::Websockets,
            signaling_protocol: None,
        });
        let error = planned_entrypoints(domain_models(source, endpoint))
            .expect_err("there is no implicit wire protocol for acknowledged frames");
        assert!(error.contains::<EntrypointPlanError>());
        assert!(format!("{error:#}").contains("requires a signaling protocol"));
    }

    #[test]
    fn plans_kafka_domain_offsets_for_the_scheduled_primary() {
        let source = IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
            client: named("upstream"),
            topic: named("events"),
            offset_mode: KafkaOffsetMode::Domain,
            instances: nonzero!(2u64),
            mode: KafkaIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        }));
        let domain = named::<DomainName>("sales");
        let mut nodes = unplaced_schedule(domain_models(source, kafka_case().source_model));
        nodes
            .get_mut(&NodeRef::new(
                nervix_models::ModelKind::Ingestor,
                named::<ModelName>("source"),
            ))
            .assured("the fixture schedules the ingestor")
            .single_unit_mut()
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
            &transport_plan(plan).source,
            IngestorConnectorPlan::Kafka(KafkaIngestorStartPlan::Topic(source)) if source.offsets == domain_offsets
        ));
        assert!(plan.keeps_domain_offsets());
    }

    #[test]
    fn retirement_follows_the_mqtt_input_and_preserves_codec_only_changes() {
        let case = mqtt_case();
        let plans = planned_entrypoints(domain_models(case.source, case.source_model))
            .assured("a matching source and ingestor produce a plan");
        let previous = start_plan(&plans);
        assert!(previous.requires_entity_retirement(None));
        assert!(!previous.requires_entity_retirement(Some(previous)));

        let mut input = transport_plan(previous).clone();
        input.codec = named("alternate_json");
        let mut replacement = previous.clone();
        replacement.input = IngestorInputPlan::Transport(input.clone());
        assert!(!previous.requires_entity_retirement(Some(&replacement)));

        let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(source)) = &mut input.source
        else {
            panic!("the MQTT fixture produces a typed MQTT source");
        };
        source.topic = nervix_models::MqttTopicFilter::parse("replacement/#")
            .assured("the replacement topic filter is valid");
        replacement.input = IngestorInputPlan::Transport(input.clone());
        assert!(previous.requires_entity_retirement(Some(&replacement)));

        input.source = http_case().plan;
        replacement.input = IngestorInputPlan::Transport(input);
        assert!(previous.requires_entity_retirement(Some(&replacement)));
        assert!(!replacement.requires_entity_retirement(None));
    }

    #[test]
    fn retirement_checks_changed_owned_pulsar_inputs_and_keeps_unchanged_inputs() {
        let case = pulsar_case();
        let plans = planned_entrypoints(domain_models(case.source, case.source_model))
            .assured("a matching source and ingestor produce a plan");
        let owned = start_plan(&plans);
        assert!(owned.requires_entity_retirement(None));
        assert!(!owned.requires_entity_retirement(Some(owned)));

        // Codec and route changes do not change the source that reads the subscription.
        let mut replacement = owned.clone();
        let mut input = transport_plan(owned).clone();
        input.codec = named("alternate_json");
        replacement.input = IngestorInputPlan::Transport(input);
        assert!(!owned.requires_entity_retirement(Some(&replacement)));

        let mut input = transport_plan(owned).clone();
        let IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) =
            &mut input.source
        else {
            panic!("the Pulsar fixture produces a typed Pulsar source");
        };
        source.subscription.ownership = PulsarSubscriptionOwnership::Ingestor {
            start_at: nervix_models::PulsarStartPosition::Latest,
        };
        let mut latest = owned.clone();
        latest.input = IngestorInputPlan::Transport(input);
        assert!(latest.requires_entity_retirement(None));
        assert!(!latest.requires_entity_retirement(Some(&latest)));

        // Changed sources require the composition root to compare their resolved subscriptions.
        let mut input = transport_plan(owned).clone();
        let IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) =
            &mut input.source
        else {
            panic!("the Pulsar fixture produces a typed Pulsar source");
        };
        source.subscription.name = named("replacement");
        let mut replacement = owned.clone();
        replacement.input = IngestorInputPlan::Transport(input.clone());
        assert!(owned.requires_entity_retirement(Some(&replacement)));
        input.source = http_case().plan;
        replacement.input = IngestorInputPlan::Transport(input);
        assert!(owned.requires_entity_retirement(Some(&replacement)));

        // A provisioned subscription is the operator's, so even a drop keeps it.
        let mut provisioned = owned.clone();
        let mut input = transport_plan(owned).clone();
        let IngestorConnectorPlan::Pulsar(PulsarIngestorStartPlan::Topic(source)) =
            &mut input.source
        else {
            panic!("the Pulsar fixture produces a typed Pulsar source");
        };
        source.subscription.ownership = PulsarSubscriptionOwnership::Provisioned;
        provisioned.input = IngestorInputPlan::Transport(input);
        assert!(!provisioned.requires_entity_retirement(None));
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
                .declared_input
                .supports_quiesce(&IngestQuiesceMode::Suspend)
        );
        assert!(
            plan.ingestor
                .declared_input
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
        #[case] case: IngestorCase,
        #[case] different: Model,
    ) {
        let error = IngestorConnectorPlan::decide(
            &named("source"),
            &case.source,
            &different,
            &nervix_models::ScheduledNode::new(
                ingestor_model(case.source.clone()),
                nervix_models::SchemaFingerprint::from_digest([1; 32]),
            ),
        )
        .expect_err("a differently named source must not be planned");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InputIdentityMismatch {
                ingestor: named("source"),
                expected: named("upstream"),
                resolved: named("different"),
            }
        );
    }

    /// A client ingestor reading batches of `payload` into `events`, under the policy durations
    /// its statement declares.
    fn client_ingestor_model(ack_timeout: &str, backoff: &str, max_backoff: &str) -> Model {
        Model::Ingestor(CreateIngestor {
            name: named("source"),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("events")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            input: nervix_models::IngestorInput::Client(nervix_models::ClientProducerIngestor {
                schema: named("payload"),
                mode: nervix_models::ClientIngestMode {
                    window: AckWindow::Parallel {
                        max: nonzero!(4u64),
                    },
                    ack_timeout: ack_timeout.to_string(),
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: backoff.to_string(),
                        max_backoff: max_backoff.to_string(),
                    },
                },
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        })
    }

    /// The client input of a fixture ingestor that reads client batches.
    fn client_plan(plan: &IngestorStartPlan) -> &ClientIngestorStartPlan {
        let IngestorInputPlan::Client(client) = &plan.input else {
            panic!(
                "the fixture ingestor reads client batches, planned as {:?}",
                plan.input
            );
        };
        client
    }

    #[test]
    fn plans_a_client_ingestor_with_its_schema_policy_and_window() {
        let models = vec![
            schema("payload"),
            relay("events", "payload"),
            client_ingestor_model("30s", "100ms", "1s"),
        ];

        let plans = planned_entrypoints(models).assured("a validated client ingestor plans");
        let plan = client_plan(start_plan(&plans));

        assert_eq!(plan.schema_name, named("payload"));
        assert_eq!(
            plan.policy,
            ClientProducerPolicy {
                window: AckWindow::Parallel {
                    max: nonzero!(4u64),
                },
                ack_timeout: Duration::from_secs(30),
                retry_backoff: Duration::from_millis(100),
                retry_max_backoff: Duration::from_secs(1),
            }
        );
        assert_eq!(plan.window_size().get(), 4);
        assert_eq!(plan, &plan.clone(), "plans compare by what they decide");
    }

    #[rstest]
    #[case::unparsable_timeout("soon", "100ms", "1s", "ACK TIMEOUT", "soon")]
    #[case::too_long_timeout(
        "18446744073709551615s 1000000000ns",
        "100ms",
        "1s",
        "ACK TIMEOUT",
        "18446744073709551615s 1000000000ns"
    )]
    #[case::zero_timeout("0s", "100ms", "1s", "ACK TIMEOUT", "0s")]
    #[case::zero_backoff("30s", "0ms", "1s", "RETRY POLICY BACKOFF", "0ms")]
    #[case::unparsable_maximum("30s", "100ms", "later", "RETRY POLICY MAX", "later")]
    fn rejects_a_client_policy_duration_that_is_not_positive(
        #[case] ack_timeout: &str,
        #[case] backoff: &str,
        #[case] max_backoff: &str,
        #[case] clause: &'static str,
        #[case] value: &str,
    ) {
        let models = vec![
            schema("payload"),
            relay("events", "payload"),
            client_ingestor_model(ack_timeout, backoff, max_backoff),
        ];

        let error = planned_entrypoints(models).expect_err("the registry admits no such duration");

        assert_eq!(
            error.current_context(),
            &EntrypointPlanError::InvalidClientDuration {
                ingestor: named("source"),
                clause,
                value: value.to_string(),
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
            &EntrypointPlanError::InputKindMismatch {
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
            &EntrypointPlanError::MissingReference {
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
