//! Incomplete source settings for a visual ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** Source-specific fields, explicit delivery and quiesce selections, and their
//!   conversion to one current `IngestSource` variant.
//! - **Depends on.** Source vocabulary, typed names, and selected browser references.
//! - **Must not know.** Connector drivers, registry snapshots, or runtime quiescence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    ClientName, ClientSourceEntity, DomainClockPeriod, EndpointIngestMode, EndpointName,
    HttpEndpointSource, HttpRequestIngestMode, HttpRequestSource, HttpSourceEntity,
    IngestQuiesceMode, IngestQuiesceOverflow, IngestSource, IngestSourceKind,
    KafkaConsumerGroupName, KafkaIngestMode, KafkaOffsetMode, KafkaSourceEntity, KafkaTopicName,
    KafkaTopicSource, ModelName, MqttIngestMode, MqttQos, MqttSession, MqttSourceEntity,
    MqttTopicFilter, MqttTopicSource, NatsIngestMode, NatsQueueGroupName, NatsSourceEntity,
    NatsSubjectName, NatsSubjectSource, NodeRef, PrometheusIngestMode, PrometheusQuerySource,
    PrometheusSourceEntity, PulsarSourceEntity, PulsarSubscription, PulsarSubscriptionName,
    PulsarSubscriptionOwnership, PulsarTopicName, PulsarTopicSource, RabbitMqIngestMode,
    RabbitMqQueueName, RabbitMqQueueSource, RabbitMqSourceEntity, RedisChannelName,
    RedisPubSubIngestMode, RedisPubSubSource, RedisSourceEntity, RetryPolicy, SqsIngestMode,
    SqsQueueName, SqsQueueSource, SqsSourceEntity, SyslogIngestMode, SyslogListenerSource,
    SyslogSourceEntity, WebsocketsConnectionSource, WebsocketsEndpointSource, WebsocketsIngestMode,
    WebsocketsSourceEntity, ZeroMqIngestMode, ZeroMqPullSource, ZeroMqSourceEntity,
};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryChoice {
    AckSequential,
    AckParallel,
    NoAck,
}

impl DeliveryChoice {
    pub(super) const fn key(self) -> &'static str {
        match self {
            Self::AckSequential => "ack-sequential",
            Self::AckParallel => "ack-parallel",
            Self::NoAck => "no-ack",
        }
    }

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::AckSequential => "ACK SEQUENTIAL",
            Self::AckParallel => "ACK PARALLEL",
            Self::NoAck => "NO ACK",
        }
    }

    pub(super) const fn acknowledged(self) -> bool {
        matches!(self, Self::AckSequential | Self::AckParallel)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QuiesceChoice {
    Suspend,
    Buffer,
    Drop,
    Reject,
}

impl QuiesceChoice {
    pub(super) const fn key(self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::Buffer => "buffer",
            Self::Drop => "drop",
            Self::Reject => "reject",
        }
    }

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Suspend => "SUSPEND",
            Self::Buffer => "BUFFER",
            Self::Drop => "DROP",
            Self::Reject => "REJECT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OffsetChoice {
    Domain,
    ConsumerGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct IngestSourceDraft {
    pub(super) kind: Option<IngestSourceKind>,
    pub(super) reference: Option<SelectedReference<ModelName>>,
    pub(super) topic: String,
    pub(super) queue: String,
    pub(super) channel: String,
    pub(super) subject: String,
    pub(super) queue_group: String,
    pub(super) subscription: String,
    pub(super) subscription_ownership: Option<PulsarSubscriptionOwnership>,
    pub(super) query: String,
    pub(super) every: String,
    pub(super) instances: String,
    pub(super) offset: Option<OffsetChoice>,
    pub(super) consumer_group: String,
    pub(super) delivery: Option<DeliveryChoice>,
    pub(super) ack_max: String,
    pub(super) batch_timeout: String,
    pub(super) ack_timeout: String,
    pub(super) retry_backoff: String,
    pub(super) retry_max: String,
    pub(super) mqtt_session: Option<MqttSession>,
    pub(super) mqtt_qos: Option<MqttQos>,
    pub(super) quiesce: Option<QuiesceChoice>,
    pub(super) buffer_size: String,
    pub(super) overflow: Option<IngestQuiesceOverflow>,
    pub(super) retry_after: String,
}

impl IngestSourceDraft {
    pub(super) fn choose_kind(&mut self, kind: IngestSourceKind) {
        if self.kind != Some(kind) {
            *self = Self {
                kind: Some(kind),
                ..Self::default()
            };
        }
    }

    pub(super) fn select_reference(&mut self, node: &NodeRef) {
        let Some(kind) = self.kind else {
            return;
        };
        let expected = kind.capabilities().reference.model_kind();
        if node.kind == expected {
            self.reference = Some(SelectedReference::chosen(node.identifier.clone()));
        }
    }

    pub(super) fn selects_reference(&self, node: &NodeRef) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        let expected = kind.capabilities().reference.model_kind();
        self.reference.as_ref().is_some_and(|reference| {
            reference.is_current() && node.kind == expected && reference.name() == &node.identifier
        })
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(reference) = &mut self.reference {
            reference.invalidate();
        }
    }

    /// Whether the chosen pair reads an endpoint this cluster serves rather than a client.
    pub(super) fn reads_endpoint(&self) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        kind.capabilities().reference.endpoint_type().is_some()
    }

    pub(super) fn current_reference(&self) -> Option<&ModelName> {
        self.reference
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn available_modes(&self) -> &'static [DeliveryChoice] {
        match self.kind {
            Some(
                IngestSourceKind::Kafka(KafkaSourceEntity::Topic)
                | IngestSourceKind::Pulsar(PulsarSourceEntity::Topic),
            ) => &[
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
                DeliveryChoice::NoAck,
            ],
            Some(IngestSourceKind::Mqtt(MqttSourceEntity::Topic)) => &[
                DeliveryChoice::NoAck,
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
            ],
            Some(
                IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue)
                | IngestSourceKind::Sqs(SqsSourceEntity::Queue),
            ) => &[DeliveryChoice::AckSequential],
            Some(
                IngestSourceKind::Http(HttpSourceEntity::Request | HttpSourceEntity::Endpoint)
                | IngestSourceKind::Nats(NatsSourceEntity::Subject)
                | IngestSourceKind::Redis(RedisSourceEntity::PubSub)
                | IngestSourceKind::Prometheus(PrometheusSourceEntity::Query)
                | IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull)
                | IngestSourceKind::Websockets(
                    WebsocketsSourceEntity::Connection | WebsocketsSourceEntity::Endpoint,
                )
                | IngestSourceKind::Syslog(SyslogSourceEntity::Listener),
            ) => &[DeliveryChoice::NoAck],
            Some(IngestSourceKind::Client(ClientSourceEntity::Producer)) | None => &[],
        }
    }

    pub(super) fn available_quiesce(&self) -> &'static [QuiesceChoice] {
        match self.kind {
            Some(
                IngestSourceKind::Kafka(KafkaSourceEntity::Topic)
                | IngestSourceKind::Pulsar(PulsarSourceEntity::Topic)
                | IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue)
                | IngestSourceKind::Sqs(SqsSourceEntity::Queue),
            ) => &[QuiesceChoice::Suspend],
            Some(IngestSourceKind::Mqtt(MqttSourceEntity::Topic)) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(
                IngestSourceKind::Nats(NatsSourceEntity::Subject)
                | IngestSourceKind::Redis(RedisSourceEntity::PubSub)
                | IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection),
            ) => &[QuiesceChoice::Buffer, QuiesceChoice::Drop],
            Some(
                IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull)
                | IngestSourceKind::Syslog(SyslogSourceEntity::Listener),
            ) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(
                IngestSourceKind::Http(HttpSourceEntity::Request)
                | IngestSourceKind::Prometheus(PrometheusSourceEntity::Query),
            ) => &[QuiesceChoice::Suspend, QuiesceChoice::Buffer],
            Some(
                IngestSourceKind::Http(HttpSourceEntity::Endpoint)
                | IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint),
            ) => &[QuiesceChoice::Reject, QuiesceChoice::Buffer],
            Some(IngestSourceKind::Client(ClientSourceEntity::Producer)) | None => &[],
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<IngestSource, IngestSourceDraftError> {
        let kind = self
            .kind
            .ok_or_else(|| Report::new(IngestSourceDraftError::Kind))?;
        // The form offers transport sources only; a client source names no client or endpoint.
        if let IngestSourceKind::Client(ClientSourceEntity::Producer) = kind {
            return Err(Report::new(IngestSourceDraftError::ClientSource));
        }
        if self.current_reference().is_none() {
            return Err(Report::new(if self.reference.is_some() {
                IngestSourceDraftError::ReferenceChanged
            } else {
                IngestSourceDraftError::Reference
            }));
        }
        let quiesce = self.build_quiesce()?;
        let source: IngestSource = match kind {
            IngestSourceKind::Client(ClientSourceEntity::Producer) => {
                return Err(Report::new(IngestSourceDraftError::ClientSource));
            }
            IngestSourceKind::Http(HttpSourceEntity::Request) => HttpRequestSource {
                client: self.client()?,
                every: self.every()?,
                mode: HttpRequestIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::Http(HttpSourceEntity::Endpoint) => HttpEndpointSource {
                endpoint: self.endpoint()?,
                mode: EndpointIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::Kafka(KafkaSourceEntity::Topic) => KafkaTopicSource {
                client: self.client()?,
                topic: KafkaTopicName::parse(&self.topic)
                    .change_context(IngestSourceDraftError::Topic)?,
                offset_mode: self.offset_mode()?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            }
            .into(),
            IngestSourceKind::Pulsar(PulsarSourceEntity::Topic) => PulsarTopicSource {
                client: self.client()?,
                topic: PulsarTopicName::parse(&self.topic)
                    .change_context(IngestSourceDraftError::Topic)?,
                subscription: self.pulsar_subscription()?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            }
            .into(),
            IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => MqttTopicSource {
                client: self.client()?,
                topic: MqttTopicFilter::parse(&self.topic)
                    .change_context(IngestSourceDraftError::Topic)?,
                instances: self.instances()?,
                mode: self.mqtt_mode()?,
                quiesce,
            }
            .into(),
            IngestSourceKind::Nats(NatsSourceEntity::Subject) => NatsSubjectSource {
                client: self.client()?,
                subject: NatsSubjectName::parse(&self.subject)
                    .change_context(IngestSourceDraftError::Subject)?,
                queue_group: NatsQueueGroupName::parse(&self.queue_group)
                    .change_context(IngestSourceDraftError::QueueGroup)?,
                instances: self.instances()?,
                mode: NatsIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue) => RabbitMqQueueSource {
                client: self.client()?,
                queue: RabbitMqQueueName::parse(&self.queue)
                    .change_context(IngestSourceDraftError::Queue)?,
                instances: self.instances()?,
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            }
            .into(),
            IngestSourceKind::Redis(RedisSourceEntity::PubSub) => RedisPubSubSource {
                client: self.client()?,
                channel: RedisChannelName::parse(&self.channel)
                    .change_context(IngestSourceDraftError::Channel)?,
                mode: RedisPubSubIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::Prometheus(PrometheusSourceEntity::Query) => PrometheusQuerySource {
                client: self.client()?,
                query: self.required(&self.query, IngestSourceDraftError::Query)?,
                every: self.every()?,
                mode: PrometheusIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull) => ZeroMqPullSource {
                client: self.client()?,
                mode: ZeroMqIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestSourceKind::Sqs(SqsSourceEntity::Queue) => SqsQueueSource {
                client: self.client()?,
                queue: SqsQueueName::parse(&self.queue)
                    .change_context(IngestSourceDraftError::Queue)?,
                instances: self.instances()?,
                mode: SqsIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            }
            .into(),
            IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection) => {
                WebsocketsConnectionSource {
                    client: self.client()?,
                    headers: Vec::new(),
                    mode: WebsocketsIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint) => {
                WebsocketsEndpointSource {
                    endpoint: self.endpoint()?,
                    mode: EndpointIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestSourceKind::Syslog(SyslogSourceEntity::Listener) => SyslogListenerSource {
                client: self.client()?,
                mode: SyslogIngestMode::NoAck,
                quiesce,
            }
            .into(),
        };
        if !source.supports_quiesce(source.quiesce()) {
            return Err(Report::new(IngestSourceDraftError::UnsupportedQuiesce));
        }
        Ok(source)
    }

    fn client(&self) -> error_stack::Result<ClientName, IngestSourceDraftError> {
        let reference = self.current_reference().ok_or_else(|| {
            Report::new(if self.reference.is_some() {
                IngestSourceDraftError::ReferenceChanged
            } else {
                IngestSourceDraftError::Reference
            })
        })?;
        Ok(ClientName::from(reference))
    }

    fn endpoint(&self) -> error_stack::Result<EndpointName, IngestSourceDraftError> {
        let reference = self.current_reference().ok_or_else(|| {
            Report::new(if self.reference.is_some() {
                IngestSourceDraftError::ReferenceChanged
            } else {
                IngestSourceDraftError::Reference
            })
        })?;
        Ok(EndpointName::from(reference))
    }

    fn required(
        &self,
        value: &str,
        error: IngestSourceDraftError,
    ) -> error_stack::Result<String, IngestSourceDraftError> {
        if value.trim().is_empty() {
            return Err(Report::new(error));
        }
        Ok(value.trim().to_string())
    }

    fn instances(&self) -> error_stack::Result<NonZeroU64, IngestSourceDraftError> {
        self.instances
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestSourceDraftError::Instances))
    }

    fn every(&self) -> error_stack::Result<DomainClockPeriod, IngestSourceDraftError> {
        self.every
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestSourceDraftError::Every))
    }

    /// The subscription a Pulsar source reads through, and who the draft says owns it, which
    /// every statement declares.
    fn pulsar_subscription(
        &self,
    ) -> error_stack::Result<PulsarSubscription, IngestSourceDraftError> {
        let name = PulsarSubscriptionName::parse(&self.subscription)
            .change_context(IngestSourceDraftError::Subscription)?;
        let Some(ownership) = self.subscription_ownership else {
            return Err(Report::new(IngestSourceDraftError::SubscriptionOwnership));
        };
        Ok(PulsarSubscription { name, ownership })
    }

    fn offset_mode(&self) -> error_stack::Result<KafkaOffsetMode, IngestSourceDraftError> {
        match self.offset {
            Some(OffsetChoice::Domain) => Ok(KafkaOffsetMode::Domain),
            Some(OffsetChoice::ConsumerGroup) => Ok(KafkaOffsetMode::ConsumerGroup(
                KafkaConsumerGroupName::parse(&self.consumer_group)
                    .change_context(IngestSourceDraftError::ConsumerGroup)?,
            )),
            None => Err(Report::new(IngestSourceDraftError::Offset)),
        }
    }

    fn retry_policy(&self) -> error_stack::Result<RetryPolicy, IngestSourceDraftError> {
        Ok(RetryPolicy {
            backoff: self.required(&self.retry_backoff, IngestSourceDraftError::RetryBackoff)?,
            max_backoff: self.required(&self.retry_max, IngestSourceDraftError::RetryMax)?,
        })
    }

    fn ack_timeout(&self) -> error_stack::Result<String, IngestSourceDraftError> {
        self.required(&self.ack_timeout, IngestSourceDraftError::AckTimeout)
    }

    fn ack_max(&self) -> error_stack::Result<NonZeroU64, IngestSourceDraftError> {
        self.ack_max
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestSourceDraftError::AckMax))
    }

    fn kafka_mode(&self) -> error_stack::Result<KafkaIngestMode, IngestSourceDraftError> {
        match self.delivery {
            Some(DeliveryChoice::AckSequential) => Ok(KafkaIngestMode::AckSequential {
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(DeliveryChoice::AckParallel) => Ok(KafkaIngestMode::AckParallel {
                max: self.ack_max()?,
                batch_timeout: self
                    .required(&self.batch_timeout, IngestSourceDraftError::BatchTimeout)?,
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(DeliveryChoice::NoAck) => Ok(KafkaIngestMode::NoAck),
            None => Err(Report::new(IngestSourceDraftError::Mode)),
        }
    }

    fn mqtt_mode(&self) -> error_stack::Result<MqttIngestMode, IngestSourceDraftError> {
        match self.delivery {
            Some(DeliveryChoice::NoAck) => {
                let session = self
                    .mqtt_session
                    .ok_or_else(|| Report::new(IngestSourceDraftError::MqttSession))?;
                let qos = self
                    .mqtt_qos
                    .ok_or_else(|| Report::new(IngestSourceDraftError::MqttQos))?;
                Ok(MqttIngestMode::NoAck { session, qos })
            }
            Some(DeliveryChoice::AckSequential | DeliveryChoice::AckParallel) => {
                if self.mqtt_session != Some(MqttSession::Persistent)
                    || self.mqtt_qos != Some(MqttQos::AtLeastOnce)
                {
                    return Err(Report::new(IngestSourceDraftError::MqttAckContract));
                }
                if self.delivery == Some(DeliveryChoice::AckSequential) {
                    Ok(MqttIngestMode::AckSequential {
                        timeout: self.ack_timeout()?,
                        retry_policy: self.retry_policy()?,
                    })
                } else {
                    Ok(MqttIngestMode::AckParallel {
                        max: self.ack_max()?,
                        batch_timeout: self
                            .required(&self.batch_timeout, IngestSourceDraftError::BatchTimeout)?,
                        timeout: self.ack_timeout()?,
                        retry_policy: self.retry_policy()?,
                    })
                }
            }
            None => Err(Report::new(IngestSourceDraftError::Mode)),
        }
    }

    fn build_quiesce(&self) -> error_stack::Result<IngestQuiesceMode, IngestSourceDraftError> {
        let choice = self
            .quiesce
            .ok_or_else(|| Report::new(IngestSourceDraftError::Quiesce))?;
        if !self.available_quiesce().contains(&choice) {
            return Err(Report::new(IngestSourceDraftError::UnsupportedQuiesce));
        }
        match choice {
            QuiesceChoice::Suspend => Ok(IngestQuiesceMode::Suspend),
            QuiesceChoice::Drop => Ok(IngestQuiesceMode::Drop),
            QuiesceChoice::Reject => Ok(IngestQuiesceMode::Reject {
                retry_after: self
                    .required(&self.retry_after, IngestSourceDraftError::RetryAfter)?,
            }),
            QuiesceChoice::Buffer => {
                let max_size =
                    self.required(&self.buffer_size, IngestSourceDraftError::BufferSize)?;
                // An endpoint holds what arrives in a bounded buffer and has no older message to
                // drop, so its buffer names no overflow.
                if self.reads_endpoint() {
                    Ok(IngestQuiesceMode::EndpointBuffer { max_size })
                } else {
                    let overflow = self
                        .overflow
                        .ok_or_else(|| Report::new(IngestSourceDraftError::Overflow))?;
                    Ok(IngestQuiesceMode::Buffer { max_size, overflow })
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum IngestSourceDraftError {
    #[error("Choose an ingestor source type")]
    Kind,
    #[error("A client ingestor reads the batches applications publish and is created in NSPL")]
    ClientSource,
    #[error("Choose a source client or endpoint")]
    Reference,
    #[error("The selected source belongs to a changed context; select it again")]
    ReferenceChanged,
    #[error("Enter a valid topic or topic filter")]
    Topic,
    #[error("Enter a valid queue")]
    Queue,
    #[error("Enter a valid channel")]
    Channel,
    #[error("Enter a valid subject")]
    Subject,
    #[error("Enter a valid queue group")]
    QueueGroup,
    #[error("Enter a valid subscription")]
    Subscription,
    #[error("Choose whether the ingestor owns the subscription or an operator provisioned it")]
    SubscriptionOwnership,
    #[error("Enter a Prometheus query")]
    Query,
    #[error("EVERY must be a positive domain-clock duration")]
    Every,
    #[error("INSTANCES must be a positive integer")]
    Instances,
    #[error("Choose Kafka offset ownership")]
    Offset,
    #[error("Enter a valid consumer group")]
    ConsumerGroup,
    #[error("Choose a supported source delivery mode")]
    Mode,
    #[error("ACK PARALLEL MAX must be a positive integer")]
    AckMax,
    #[error("Enter BATCH TIMEOUT")]
    BatchTimeout,
    #[error("Enter ACK TIMEOUT")]
    AckTimeout,
    #[error("Enter RETRY POLICY BACKOFF")]
    RetryBackoff,
    #[error("Enter RETRY POLICY MAX")]
    RetryMax,
    #[error("Choose MQTT SESSION")]
    MqttSession,
    #[error("Choose MQTT QOS")]
    MqttQos,
    #[error("MQTT ACK requires SESSION PERSISTENT QOS 1")]
    MqttAckContract,
    #[error("Choose an ON QUIESCE mode")]
    Quiesce,
    #[error("This source cannot use the selected ON QUIESCE mode")]
    UnsupportedQuiesce,
    #[error("BUFFER MAX SIZE must be positive")]
    BufferSize,
    #[error("Choose ON OVERFLOW DROP OLDEST or DROP NEWEST")]
    Overflow,
    #[error("Enter REJECT RETRY AFTER")]
    RetryAfter,
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CodecName, CreateStatement, GeneralErrorPolicy, HttpSourceEntity, IngestQuiesceOverflow,
        IngestSource, IngestSourceKind, KafkaSourceEntity, Model, ModelKind, ModelName, MqttQos,
        MqttSession, MqttSourceEntity, NatsSourceEntity, NodeRef, PulsarSource, PulsarSourceEntity,
        PulsarSubscriptionOwnership, RedisSourceEntity, RequestedResourceVersion, Statement,
        WebsocketsSourceEntity,
    };
    use nervix_nspl::statement::parse_statement;

    use super::{DeliveryChoice, IngestSourceDraft, OffsetChoice, QuiesceChoice};
    use crate::create_dialog::{
        SelectedReference,
        ingestor_draft::{IngestorDraft, TimestampDraft},
        ingestor_route_draft::{FlushDraft, InheritDraft, MessageErrorDraft},
    };

    fn complete(kind: IngestSourceKind) -> IngestSourceDraft {
        let mut draft = IngestSourceDraft::default();
        draft.choose_kind(kind);
        let reference_kind = kind.capabilities().reference.model_kind();
        draft.select_reference(&NodeRef::new(
            reference_kind,
            ModelName::parse("source").assured("valid source name"),
        ));
        draft.topic = "events".into();
        draft.queue = "events".into();
        draft.channel = "events".into();
        draft.subject = "events".into();
        draft.queue_group = "workers".into();
        draft.subscription = "workers".into();
        draft.subscription_ownership = Some(PulsarSubscriptionOwnership::Provisioned);
        draft.query = "up".into();
        draft.every = "1s".into();
        draft.instances = "1".into();
        draft.offset = Some(OffsetChoice::Domain);
        draft.delivery = Some(match kind {
            IngestSourceKind::Kafka(KafkaSourceEntity::Topic)
            | IngestSourceKind::Pulsar(PulsarSourceEntity::Topic)
            | IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => DeliveryChoice::NoAck,
            _ => DeliveryChoice::AckSequential,
        });
        draft.ack_max = "2".into();
        draft.batch_timeout = "1s".into();
        draft.ack_timeout = "30s".into();
        draft.retry_backoff = "200ms".into();
        draft.retry_max = "5s".into();
        draft.mqtt_session = Some(MqttSession::Clean);
        draft.mqtt_qos = Some(MqttQos::AtMostOnce);
        draft.quiesce = Some(match kind {
            IngestSourceKind::Http(HttpSourceEntity::Endpoint)
            | IngestSourceKind::Websockets(WebsocketsSourceEntity::Endpoint) => {
                QuiesceChoice::Reject
            }
            IngestSourceKind::Nats(NatsSourceEntity::Subject)
            | IngestSourceKind::Redis(RedisSourceEntity::PubSub)
            | IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection) => {
                QuiesceChoice::Drop
            }
            IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => QuiesceChoice::Buffer,
            _ => QuiesceChoice::Suspend,
        });
        draft.buffer_size = "1MiB".into();
        draft.overflow = Some(IngestQuiesceOverflow::DropOldest);
        draft.retry_after = "5s".into();
        draft
    }

    #[test]
    fn every_offered_source_builds_its_current_variant_with_its_required_contract() {
        for kind in IngestSourceKind::modeled() {
            let draft = complete(kind);
            assert_eq!(
                draft.build().assured("complete source draft builds").kind(),
                kind,
                "{}",
                kind.form_label(),
            );
            let mut ingestor = IngestorDraft {
                name: "source_in".into(),
                source: draft,
                codec: Some(SelectedReference::chosen(
                    CodecName::parse("decode").assured("valid codec"),
                )),
                timestamp: TimestampDraft::Now,
                ..Default::default()
            };
            ingestor.routes[0].select_relay(&NodeRef::new(
                ModelKind::Relay,
                ModelName::parse("out").assured("valid relay"),
            ));
            ingestor.routes[0].inherit = InheritDraft::All;
            ingestor.routes[0].branch.choose_unbranched();
            ingestor.routes[0].flush = FlushDraft::Immediate;
            ingestor.routes[0].message_error = MessageErrorDraft::Log;
            ingestor.general_error = Some(GeneralErrorPolicy::Log);
            let statement = Statement::Create(CreateStatement::new(
                Box::new(Model::<RequestedResourceVersion>::Ingestor(
                    ingestor.build().assured("complete ingestor builds"),
                )),
                false,
            ));
            let canonical = statement.to_canonical_nspl().assured("ingestor renders");
            assert_eq!(
                parse_statement(&canonical).assured("canonical ingestor parses"),
                statement,
                "{} canonical round trip",
                kind.form_label(),
            );
        }
    }

    #[test]
    fn acknowledged_mqtt_and_quiesce_require_matching_session_and_qos() {
        let mut draft = complete(IngestSourceKind::Mqtt(MqttSourceEntity::Topic));
        draft.delivery = Some(DeliveryChoice::AckParallel);
        draft.quiesce = Some(QuiesceChoice::Suspend);
        assert!(draft.build().is_err());
        draft.mqtt_session = Some(MqttSession::Persistent);
        draft.mqtt_qos = Some(MqttQos::AtLeastOnce);
        assert!(draft.build().is_ok());
        draft.delivery = Some(DeliveryChoice::NoAck);
        draft.mqtt_session = Some(MqttSession::Clean);
        assert!(draft.build().is_err());
    }

    #[test]
    fn every_source_quiesce_choice_builds_only_when_its_contract_is_met() {
        for kind in IngestSourceKind::modeled() {
            let draft = complete(kind);
            for choice in draft.available_quiesce() {
                let mut candidate = draft.clone();
                candidate.quiesce = Some(*choice);
                if kind == IngestSourceKind::Mqtt(MqttSourceEntity::Topic)
                    && *choice == QuiesceChoice::Suspend
                {
                    candidate.mqtt_session = Some(MqttSession::Persistent);
                    candidate.mqtt_qos = Some(MqttQos::AtLeastOnce);
                }
                assert!(
                    candidate.build().is_ok(),
                    "{} {}",
                    kind.form_label(),
                    choice.label()
                );
            }
        }
        let mut kafka = complete(IngestSourceKind::Kafka(KafkaSourceEntity::Topic));
        kafka.quiesce = Some(QuiesceChoice::Buffer);
        assert!(kafka.build().is_err());
        let mut endpoint = complete(IngestSourceKind::Http(HttpSourceEntity::Endpoint));
        endpoint.quiesce = Some(QuiesceChoice::Drop);
        assert!(endpoint.build().is_err());
    }

    #[test]
    fn a_pulsar_source_builds_only_once_its_subscription_ownership_is_chosen() {
        let mut draft = complete(IngestSourceKind::Pulsar(PulsarSourceEntity::Topic));
        draft.subscription_ownership = None;
        let error = draft
            .build()
            .expect_err("a Pulsar source declares who owns its subscription");
        assert_eq!(
            error.current_context(),
            &super::IngestSourceDraftError::SubscriptionOwnership
        );
        for ownership in [
            PulsarSubscriptionOwnership::Ingestor,
            PulsarSubscriptionOwnership::Provisioned,
        ] {
            draft.subscription_ownership = Some(ownership);
            let IngestSource::Pulsar(PulsarSource::Topic(source)) =
                draft.build().assured("a complete Pulsar draft builds")
            else {
                panic!("a Pulsar draft builds a Pulsar topic source");
            };
            assert_eq!(source.subscription.ownership, ownership);
            assert_eq!(source.subscription.name.as_str(), "workers");
        }
    }

    #[test]
    fn source_switch_clears_fields_and_stale_reference_cannot_build() {
        let mut draft = complete(IngestSourceKind::Kafka(KafkaSourceEntity::Topic));
        draft.invalidate_references();
        assert!(draft.build().is_err());
        draft.choose_kind(IngestSourceKind::Http(HttpSourceEntity::Endpoint));
        assert!(draft.reference.is_none());
        assert!(draft.topic.is_empty());
        assert!(draft.quiesce.is_none());
    }
}
