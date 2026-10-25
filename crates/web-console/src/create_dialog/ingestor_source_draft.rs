//! Incomplete source settings for a visual ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** Source-specific fields, explicit delivery and quiesce selections, and their
//!   conversion to one current `IngestorEntity` variant.
//! - **Depends on.** Source vocabulary, typed names, and selected browser references.
//! - **Must not know.** Connector drivers, registry snapshots, or runtime quiescence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    ClientIngestorEntity, ClientName, DomainClockPeriod, EndpointIngestMode, EndpointName,
    HttpEndpointIngestor, HttpIngestorEntity, HttpRequestIngestMode, HttpRequestIngestor,
    IngestQuiesceMode, IngestQuiesceOverflow, IngestorEntity, IngestorEntityKind,
    KafkaConsumerGroupName, KafkaIngestMode, KafkaIngestorEntity, KafkaOffsetMode,
    KafkaTopicIngestor, KafkaTopicName, ModelName, MqttIngestMode, MqttIngestorEntity, MqttQos,
    MqttSession, MqttTopicFilter, MqttTopicIngestor, NatsIngestMode, NatsIngestorEntity,
    NatsQueueGroupName, NatsSubjectIngestor, NatsSubjectName, NodeRef, PrometheusIngestMode,
    PrometheusIngestorEntity, PrometheusQueryIngestor, PulsarIngestorEntity, PulsarSubscription,
    PulsarSubscriptionName, PulsarSubscriptionOwnership, PulsarTopicIngestor, PulsarTopicName,
    RabbitMqIngestMode, RabbitMqIngestorEntity, RabbitMqQueueIngestor, RabbitMqQueueName,
    RedisChannelName, RedisIngestorEntity, RedisPubSubIngestMode, RedisPubSubIngestor, RetryPolicy,
    SqsIngestMode, SqsIngestorEntity, SqsQueueIngestor, SqsQueueName, SyslogIngestMode,
    SyslogIngestorEntity, SyslogListenerIngestor, WebsocketsConnectionIngestor,
    WebsocketsEndpointIngestor, WebsocketsIngestMode, WebsocketsIngestorEntity, ZeroMqIngestMode,
    ZeroMqIngestorEntity, ZeroMqPullIngestor,
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
pub(super) struct IngestorEntityDraft {
    pub(super) kind: Option<IngestorEntityKind>,
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

impl IngestorEntityDraft {
    pub(super) fn choose_kind(&mut self, kind: IngestorEntityKind) {
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
                IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)
                | IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic),
            ) => &[
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
                DeliveryChoice::NoAck,
            ],
            Some(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)) => &[
                DeliveryChoice::NoAck,
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
            ],
            Some(
                IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue)
                | IngestorEntityKind::Sqs(SqsIngestorEntity::Queue),
            ) => &[DeliveryChoice::AckSequential],
            Some(
                IngestorEntityKind::Http(
                    HttpIngestorEntity::Request | HttpIngestorEntity::Endpoint,
                )
                | IngestorEntityKind::Nats(NatsIngestorEntity::Subject)
                | IngestorEntityKind::Redis(RedisIngestorEntity::PubSub)
                | IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query)
                | IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull)
                | IngestorEntityKind::Websockets(
                    WebsocketsIngestorEntity::Connection | WebsocketsIngestorEntity::Endpoint,
                )
                | IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener),
            ) => &[DeliveryChoice::NoAck],
            Some(IngestorEntityKind::Client(ClientIngestorEntity::Producer)) | None => &[],
        }
    }

    pub(super) fn available_quiesce(&self) -> &'static [QuiesceChoice] {
        match self.kind {
            Some(
                IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)
                | IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic)
                | IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue)
                | IngestorEntityKind::Sqs(SqsIngestorEntity::Queue),
            ) => &[QuiesceChoice::Suspend],
            Some(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(
                IngestorEntityKind::Nats(NatsIngestorEntity::Subject)
                | IngestorEntityKind::Redis(RedisIngestorEntity::PubSub)
                | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection),
            ) => &[QuiesceChoice::Buffer, QuiesceChoice::Drop],
            Some(
                IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull)
                | IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener),
            ) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(
                IngestorEntityKind::Http(HttpIngestorEntity::Request)
                | IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query),
            ) => &[QuiesceChoice::Suspend, QuiesceChoice::Buffer],
            Some(
                IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)
                | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint),
            ) => &[QuiesceChoice::Reject, QuiesceChoice::Buffer],
            Some(IngestorEntityKind::Client(ClientIngestorEntity::Producer)) | None => &[],
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<IngestorEntity, IngestorEntityDraftError> {
        let kind = self
            .kind
            .ok_or_else(|| Report::new(IngestorEntityDraftError::Kind))?;
        // The form offers transport sources only; a client source names no client or endpoint.
        if let IngestorEntityKind::Client(ClientIngestorEntity::Producer) = kind {
            return Err(Report::new(IngestorEntityDraftError::ClientInput));
        }
        if self.current_reference().is_none() {
            return Err(Report::new(if self.reference.is_some() {
                IngestorEntityDraftError::ReferenceChanged
            } else {
                IngestorEntityDraftError::Reference
            }));
        }
        let quiesce = self.build_quiesce()?;
        let source: IngestorEntity = match kind {
            IngestorEntityKind::Client(ClientIngestorEntity::Producer) => {
                return Err(Report::new(IngestorEntityDraftError::ClientInput));
            }
            IngestorEntityKind::Http(HttpIngestorEntity::Request) => HttpRequestIngestor {
                client: self.client()?,
                every: self.every()?,
                mode: HttpRequestIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => HttpEndpointIngestor {
                endpoint: self.endpoint()?,
                mode: EndpointIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => KafkaTopicIngestor {
                client: self.client()?,
                topic: KafkaTopicName::parse(&self.topic)
                    .change_context(IngestorEntityDraftError::Topic)?,
                offset_mode: self.offset_mode()?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => PulsarTopicIngestor {
                client: self.client()?,
                topic: PulsarTopicName::parse(&self.topic)
                    .change_context(IngestorEntityDraftError::Topic)?,
                subscription: self.pulsar_subscription()?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => MqttTopicIngestor {
                client: self.client()?,
                topic: MqttTopicFilter::parse(&self.topic)
                    .change_context(IngestorEntityDraftError::Topic)?,
                instances: self.instances()?,
                mode: self.mqtt_mode()?,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => NatsSubjectIngestor {
                client: self.client()?,
                subject: NatsSubjectName::parse(&self.subject)
                    .change_context(IngestorEntityDraftError::Subject)?,
                queue_group: NatsQueueGroupName::parse(&self.queue_group)
                    .change_context(IngestorEntityDraftError::QueueGroup)?,
                instances: self.instances()?,
                mode: NatsIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => RabbitMqQueueIngestor {
                client: self.client()?,
                queue: RabbitMqQueueName::parse(&self.queue)
                    .change_context(IngestorEntityDraftError::Queue)?,
                instances: self.instances()?,
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            }
            .into(),
            IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => RedisPubSubIngestor {
                client: self.client()?,
                channel: RedisChannelName::parse(&self.channel)
                    .change_context(IngestorEntityDraftError::Channel)?,
                mode: RedisPubSubIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => {
                PrometheusQueryIngestor {
                    client: self.client()?,
                    query: self.required(&self.query, IngestorEntityDraftError::Query)?,
                    max_body_size: None,
                    every: self.every()?,
                    mode: PrometheusIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull) => ZeroMqPullIngestor {
                client: self.client()?,
                mode: ZeroMqIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => SqsQueueIngestor {
                client: self.client()?,
                queue: SqsQueueName::parse(&self.queue)
                    .change_context(IngestorEntityDraftError::Queue)?,
                instances: self.instances()?,
                mode: SqsIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            }
            .into(),
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
                WebsocketsConnectionIngestor {
                    client: self.client()?,
                    headers: Vec::new(),
                    mode: WebsocketsIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                WebsocketsEndpointIngestor {
                    endpoint: self.endpoint()?,
                    mode: EndpointIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => SyslogListenerIngestor {
                client: self.client()?,
                mode: SyslogIngestMode::NoAck,
                quiesce,
            }
            .into(),
        };
        if !source.supports_quiesce(source.quiesce()) {
            return Err(Report::new(IngestorEntityDraftError::UnsupportedQuiesce));
        }
        Ok(source)
    }

    fn client(&self) -> error_stack::Result<ClientName, IngestorEntityDraftError> {
        let reference = self.current_reference().ok_or_else(|| {
            Report::new(if self.reference.is_some() {
                IngestorEntityDraftError::ReferenceChanged
            } else {
                IngestorEntityDraftError::Reference
            })
        })?;
        Ok(ClientName::from(reference))
    }

    fn endpoint(&self) -> error_stack::Result<EndpointName, IngestorEntityDraftError> {
        let reference = self.current_reference().ok_or_else(|| {
            Report::new(if self.reference.is_some() {
                IngestorEntityDraftError::ReferenceChanged
            } else {
                IngestorEntityDraftError::Reference
            })
        })?;
        Ok(EndpointName::from(reference))
    }

    fn required(
        &self,
        value: &str,
        error: IngestorEntityDraftError,
    ) -> error_stack::Result<String, IngestorEntityDraftError> {
        if value.trim().is_empty() {
            return Err(Report::new(error));
        }
        Ok(value.trim().to_string())
    }

    fn instances(&self) -> error_stack::Result<NonZeroU64, IngestorEntityDraftError> {
        self.instances
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestorEntityDraftError::Instances))
    }

    fn every(&self) -> error_stack::Result<DomainClockPeriod, IngestorEntityDraftError> {
        self.every
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestorEntityDraftError::Every))
    }

    /// The subscription a Pulsar source reads through, and who the draft says owns it, which
    /// every statement declares.
    fn pulsar_subscription(
        &self,
    ) -> error_stack::Result<PulsarSubscription, IngestorEntityDraftError> {
        let name = PulsarSubscriptionName::parse(&self.subscription)
            .change_context(IngestorEntityDraftError::Subscription)?;
        let Some(ownership) = self.subscription_ownership else {
            return Err(Report::new(IngestorEntityDraftError::SubscriptionOwnership));
        };
        Ok(PulsarSubscription { name, ownership })
    }

    fn offset_mode(&self) -> error_stack::Result<KafkaOffsetMode, IngestorEntityDraftError> {
        match self.offset {
            Some(OffsetChoice::Domain) => Ok(KafkaOffsetMode::Domain),
            Some(OffsetChoice::ConsumerGroup) => Ok(KafkaOffsetMode::ConsumerGroup(
                KafkaConsumerGroupName::parse(&self.consumer_group)
                    .change_context(IngestorEntityDraftError::ConsumerGroup)?,
            )),
            None => Err(Report::new(IngestorEntityDraftError::Offset)),
        }
    }

    fn retry_policy(&self) -> error_stack::Result<RetryPolicy, IngestorEntityDraftError> {
        Ok(RetryPolicy {
            backoff: self.required(&self.retry_backoff, IngestorEntityDraftError::RetryBackoff)?,
            max_backoff: self.required(&self.retry_max, IngestorEntityDraftError::RetryMax)?,
        })
    }

    fn ack_timeout(&self) -> error_stack::Result<String, IngestorEntityDraftError> {
        self.required(&self.ack_timeout, IngestorEntityDraftError::AckTimeout)
    }

    fn ack_max(&self) -> error_stack::Result<NonZeroU64, IngestorEntityDraftError> {
        self.ack_max
            .trim()
            .parse()
            .map_err(|_| Report::new(IngestorEntityDraftError::AckMax))
    }

    fn kafka_mode(&self) -> error_stack::Result<KafkaIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(DeliveryChoice::AckSequential) => Ok(KafkaIngestMode::AckSequential {
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(DeliveryChoice::AckParallel) => Ok(KafkaIngestMode::AckParallel {
                max: self.ack_max()?,
                batch_timeout: self
                    .required(&self.batch_timeout, IngestorEntityDraftError::BatchTimeout)?,
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(DeliveryChoice::NoAck) => Ok(KafkaIngestMode::NoAck),
            None => Err(Report::new(IngestorEntityDraftError::Mode)),
        }
    }

    fn mqtt_mode(&self) -> error_stack::Result<MqttIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(DeliveryChoice::NoAck) => {
                let session = self
                    .mqtt_session
                    .ok_or_else(|| Report::new(IngestorEntityDraftError::MqttSession))?;
                let qos = self
                    .mqtt_qos
                    .ok_or_else(|| Report::new(IngestorEntityDraftError::MqttQos))?;
                Ok(MqttIngestMode::NoAck { session, qos })
            }
            Some(DeliveryChoice::AckSequential | DeliveryChoice::AckParallel) => {
                if self.mqtt_session != Some(MqttSession::Persistent)
                    || self.mqtt_qos != Some(MqttQos::AtLeastOnce)
                {
                    return Err(Report::new(IngestorEntityDraftError::MqttAckContract));
                }
                if self.delivery == Some(DeliveryChoice::AckSequential) {
                    Ok(MqttIngestMode::AckSequential {
                        timeout: self.ack_timeout()?,
                        retry_policy: self.retry_policy()?,
                    })
                } else {
                    Ok(MqttIngestMode::AckParallel {
                        max: self.ack_max()?,
                        batch_timeout: self.required(
                            &self.batch_timeout,
                            IngestorEntityDraftError::BatchTimeout,
                        )?,
                        timeout: self.ack_timeout()?,
                        retry_policy: self.retry_policy()?,
                    })
                }
            }
            None => Err(Report::new(IngestorEntityDraftError::Mode)),
        }
    }

    fn build_quiesce(&self) -> error_stack::Result<IngestQuiesceMode, IngestorEntityDraftError> {
        let choice = self
            .quiesce
            .ok_or_else(|| Report::new(IngestorEntityDraftError::Quiesce))?;
        if !self.available_quiesce().contains(&choice) {
            return Err(Report::new(IngestorEntityDraftError::UnsupportedQuiesce));
        }
        match choice {
            QuiesceChoice::Suspend => Ok(IngestQuiesceMode::Suspend),
            QuiesceChoice::Drop => Ok(IngestQuiesceMode::Drop),
            QuiesceChoice::Reject => Ok(IngestQuiesceMode::Reject {
                retry_after: self
                    .required(&self.retry_after, IngestorEntityDraftError::RetryAfter)?,
            }),
            QuiesceChoice::Buffer => {
                let max_size =
                    self.required(&self.buffer_size, IngestorEntityDraftError::BufferSize)?;
                // An endpoint holds what arrives in a bounded buffer and has no older message to
                // drop, so its buffer names no overflow.
                if self.reads_endpoint() {
                    Ok(IngestQuiesceMode::EndpointBuffer { max_size })
                } else {
                    let overflow = self
                        .overflow
                        .ok_or_else(|| Report::new(IngestorEntityDraftError::Overflow))?;
                    Ok(IngestQuiesceMode::Buffer { max_size, overflow })
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum IngestorEntityDraftError {
    #[error("Choose an ingestor entity type")]
    Kind,
    #[error("A client ingestor reads the batches applications publish and is created in Plexus")]
    ClientInput,
    #[error("Choose an ingestor client or endpoint")]
    Reference,
    #[error("The selected ingestor input belongs to a changed context; select it again")]
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
        CodecName, CreateStatement, GeneralErrorPolicy, HttpIngestorEntity, IngestQuiesceOverflow,
        IngestorEntity, IngestorEntityKind, KafkaIngestorEntity, Model, ModelKind, ModelName,
        MqttIngestorEntity, MqttQos, MqttSession, NatsIngestorEntity, NodeRef, PulsarIngestor,
        PulsarIngestorEntity, PulsarSubscriptionOwnership, RedisIngestorEntity,
        RequestedResourceVersion, Statement, WebsocketsIngestorEntity,
    };
    use nervix_plexus::statement::parse_statement;

    use super::{DeliveryChoice, IngestorEntityDraft, OffsetChoice, QuiesceChoice};
    use crate::create_dialog::{
        SelectedReference,
        ingestor_draft::{IngestorDraft, TimestampDraft},
        ingestor_route_draft::{FlushDraft, InheritDraft, MessageErrorDraft},
    };

    fn complete(kind: IngestorEntityKind) -> IngestorEntityDraft {
        let mut draft = IngestorEntityDraft::default();
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
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)
            | IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic)
            | IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => DeliveryChoice::NoAck,
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
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)
            | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                QuiesceChoice::Reject
            }
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject)
            | IngestorEntityKind::Redis(RedisIngestorEntity::PubSub)
            | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
                QuiesceChoice::Drop
            }
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => QuiesceChoice::Buffer,
            _ => QuiesceChoice::Suspend,
        });
        draft.buffer_size = "1MiB".into();
        draft.overflow = Some(IngestQuiesceOverflow::DropOldest);
        draft.retry_after = "5s".into();
        draft
    }

    #[test]
    fn every_offered_source_builds_its_current_variant_with_its_required_contract() {
        for kind in IngestorEntityKind::modeled() {
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
            let canonical = statement.to_canonical_plexus().assured("ingestor renders");
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
        let mut draft = complete(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic));
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
        for kind in IngestorEntityKind::modeled() {
            let draft = complete(kind);
            for choice in draft.available_quiesce() {
                let mut candidate = draft.clone();
                candidate.quiesce = Some(*choice);
                if kind == IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)
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
        let mut kafka = complete(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic));
        kafka.quiesce = Some(QuiesceChoice::Buffer);
        assert!(kafka.build().is_err());
        let mut endpoint = complete(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint));
        endpoint.quiesce = Some(QuiesceChoice::Drop);
        assert!(endpoint.build().is_err());
    }

    #[test]
    fn a_pulsar_source_builds_only_once_its_subscription_ownership_is_chosen() {
        let mut draft = complete(IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic));
        draft.subscription_ownership = None;
        let error = draft
            .build()
            .expect_err("a Pulsar source declares who owns its subscription");
        assert_eq!(
            error.current_context(),
            &super::IngestorEntityDraftError::SubscriptionOwnership
        );
        for ownership in [
            PulsarSubscriptionOwnership::Ingestor,
            PulsarSubscriptionOwnership::Provisioned,
        ] {
            draft.subscription_ownership = Some(ownership);
            let IngestorEntity::Pulsar(PulsarIngestor::Topic(source)) =
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
        let mut draft = complete(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic));
        draft.invalidate_references();
        assert!(draft.build().is_err());
        draft.choose_kind(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint));
        assert!(draft.reference.is_none());
        assert!(draft.topic.is_empty());
        assert!(draft.quiesce.is_none());
    }
}
