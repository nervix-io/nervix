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

use std::num::{NonZeroU16, NonZeroU64};

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    ChoiceRequirement, ClientIngestorEntity, ClientName, DomainClockPeriod, EndpointIngestMode,
    EndpointName, HttpEndpointIngestor, HttpIngestorEntity, HttpRequestHeader,
    HttpRequestIngestMode, HttpRequestIngestor, IngestDeliveryKind, IngestQuiesceKind,
    IngestQuiesceMode, IngestQuiesceOverflow, IngestorEntity, IngestorEntityKind,
    KafkaConsumerGroupName, KafkaIngestMode, KafkaIngestorEntity, KafkaOffsetMode,
    KafkaTopicIngestor, KafkaTopicName, ModelName, MqttIngestMode, MqttIngestorEntity, MqttQos,
    MqttSession, MqttTopicFilter, MqttTopicIngestor, NatsIngestMode, NatsIngestorEntity,
    NatsQueueGroupName, NatsSubjectIngestor, NatsSubjectName, NodeRef, PayloadSizeLimit,
    PrometheusIngestMode, PrometheusIngestorEntity, PrometheusQueryIngestor, PulsarIngestMode,
    PulsarIngestorEntity, PulsarSubscription, PulsarSubscriptionName, PulsarSubscriptionOwnership,
    PulsarTopicIngestor, PulsarTopicName, RabbitMqIngestMode, RabbitMqIngestorEntity,
    RabbitMqQueueIngestor, RabbitMqQueueName, RedisChannelName, RedisIngestorEntity,
    RedisPubSubIngestMode, RedisPubSubIngestor, RetryPolicy, SqsIngestMode, SqsIngestorEntity,
    SqsQueueIngestor, SqsQueueName, SyslogIngestMode, SyslogIngestorEntity, SyslogListenerIngestor,
    WebsocketsConnectionIngestor, WebsocketsEndpointIngestor, WebsocketsIngestMode,
    WebsocketsIngestorEntity, WebsocketsUpgradeHeader, WebsocketsUpgradeHeaders, ZeroMqIngestMode,
    ZeroMqIngestorEntity, ZeroMqPullIngestor,
};
use strum::IntoEnumIterator as _;
use thiserror::Error;

use super::SelectedReference;

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
    pub(super) method: String,
    pub(super) request_headers: Vec<HttpRequestHeader>,
    pub(super) max_body_size: String,
    pub(super) upgrade_headers: Vec<WebsocketsUpgradeHeader>,
    pub(super) every: String,
    pub(super) instances: String,
    pub(super) offset: Option<OffsetChoice>,
    pub(super) consumer_group: String,
    pub(super) delivery: Option<IngestDeliveryKind>,
    pub(super) ack_max: String,
    pub(super) batch_timeout: String,
    pub(super) ack_timeout: String,
    pub(super) retry_backoff: String,
    pub(super) retry_max: String,
    pub(super) mqtt_session: Option<MqttSession>,
    pub(super) mqtt_qos: Option<MqttQos>,
    pub(super) quiesce: Option<IngestQuiesceKind>,
    pub(super) buffer_size: String,
    pub(super) overflow: Option<IngestQuiesceOverflow>,
    pub(super) retry_after: String,
}

impl IngestorEntityDraft {
    pub(super) fn choose_kind(&mut self, kind: IngestorEntityKind) {
        if self.kind != Some(kind) {
            *self = Self {
                kind: Some(kind),
                delivery: kind
                    .capabilities()
                    .delivery
                    .first()
                    .map(|delivery| delivery.kind),
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

    pub(super) fn available_modes(&self) -> Vec<IngestDeliveryKind> {
        let Some(kind) = self.kind else {
            return Vec::new();
        };
        kind.capabilities()
            .delivery
            .iter()
            .map(|delivery| delivery.kind)
            .collect()
    }

    pub(super) fn available_quiesce(&self) -> Vec<IngestQuiesceKind> {
        let Some(kind) = self.kind else {
            return Vec::new();
        };
        kind.capabilities()
            .quiesce
            .iter()
            .map(|quiesce| quiesce.kind)
            .collect()
    }

    fn option_default(&self, keywords: &str) -> Option<&'static str> {
        let option = self.kind?.capabilities().option(keywords)?;
        match option.presence {
            nervix_models::OptionPresence::Required => None,
            nervix_models::OptionPresence::Optional { default } => default,
        }
    }

    pub(super) fn chosen_option(&self, keywords: &str) -> Option<&'static str> {
        let selected = match keywords {
            "SESSION" => self.mqtt_session.map(|session| session.keyword()),
            "QOS" => self.mqtt_qos.map(|qos| qos.spelling()),
            "SUBSCRIPTION" => self
                .subscription_ownership
                .map(|ownership| ownership.keywords()),
            "OFFSET BY" => self.offset.map(|offset| match offset {
                OffsetChoice::Domain => "DOMAIN",
                OffsetChoice::ConsumerGroup => "CONSUMER GROUP",
            }),
            _ => None,
        };
        selected.or_else(|| self.option_default(keywords))
    }

    pub(super) fn choose_option(&mut self, keywords: &str, choice: &str) {
        match keywords {
            "SESSION" => {
                self.mqtt_session = MqttSession::iter().find(|value| value.keyword() == choice)
            }
            "QOS" => self.mqtt_qos = MqttQos::iter().find(|value| value.spelling() == choice),
            "SUBSCRIPTION" => {
                self.subscription_ownership =
                    PulsarSubscriptionOwnership::iter().find(|value| value.keywords() == choice)
            }
            "OFFSET BY" => {
                self.offset = match choice {
                    "DOMAIN" => Some(OffsetChoice::Domain),
                    "CONSUMER GROUP" => Some(OffsetChoice::ConsumerGroup),
                    _ => None,
                }
            }
            _ => {}
        }
    }

    fn requirement_met(&self, requirement: &ChoiceRequirement) -> bool {
        let Some(chosen) = self.chosen_option(requirement.option) else {
            return false;
        };
        requirement.accepts(chosen)
    }

    pub(super) fn mode_enabled(&self, mode: IngestDeliveryKind) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        let Some(delivery) = kind.capabilities().declared_delivery(mode) else {
            return false;
        };
        delivery
            .requires
            .iter()
            .all(|requirement| self.requirement_met(requirement))
    }

    pub(super) fn quiesce_enabled(&self, choice: IngestQuiesceKind) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        let Some(mode) = self.delivery else {
            return false;
        };
        if !mode.allows_quiesce(choice) {
            return false;
        }
        let Some(quiesce) = kind.capabilities().declared_quiesce(choice) else {
            return false;
        };
        quiesce
            .requires
            .iter()
            .all(|requirement| self.requirement_met(requirement))
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
        let mode = self
            .delivery
            .ok_or_else(|| Report::new(IngestorEntityDraftError::Mode))?;
        let Some(delivery) = kind.capabilities().declared_delivery(mode) else {
            return Err(Report::new(IngestorEntityDraftError::Mode));
        };
        for requirement in delivery.requires {
            if !self.requirement_met(requirement) {
                return Err(Report::new(IngestorEntityDraftError::OptionRequirement {
                    requirement: *requirement,
                }));
            }
        }
        let quiesce = self.build_quiesce()?;
        let source: IngestorEntity = match kind {
            IngestorEntityKind::Client(ClientIngestorEntity::Producer) => {
                return Err(Report::new(IngestorEntityDraftError::ClientInput));
            }
            IngestorEntityKind::Http(HttpIngestorEntity::Request) => HttpRequestIngestor {
                client: self.client()?,
                method: self.required(&self.method, IngestorEntityDraftError::Method)?,
                headers: self.request_headers.clone(),
                max_body_size: self.max_body_size()?,
                every: self.every()?,
                mode: HttpRequestIngestMode::NoAck,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => HttpEndpointIngestor {
                endpoint: self.endpoint()?,
                mode: self.endpoint_mode()?,
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
                mode: self.pulsar_mode()?,
                quiesce,
            }
            .into(),
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => MqttTopicIngestor {
                client: self.client()?,
                topic: MqttTopicFilter::parse(&self.topic)
                    .change_context(IngestorEntityDraftError::Topic)?,
                instances: self.instances()?,
                session: self.mqtt_session()?,
                qos: self.mqtt_qos()?,
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
                mode: self.rabbitmq_mode()?,
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
                    max_body_size: self.max_body_size()?,
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
                    headers: self.build_upgrade_headers()?,
                    mode: WebsocketsIngestMode::NoAck,
                    quiesce,
                }
                .into()
            }
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                WebsocketsEndpointIngestor {
                    endpoint: self.endpoint()?,
                    mode: self.endpoint_mode()?,
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
        let value = if self.instances.trim().is_empty() {
            self.option_default("INSTANCES").unwrap_or("")
        } else {
            self.instances.trim()
        };
        value
            .parse::<NonZeroU64>()
            .map_err(|cause| Report::new(cause).change_context(IngestorEntityDraftError::Instances))
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
            .map_err(|cause| Report::new(cause).change_context(IngestorEntityDraftError::AckMax))
    }

    fn kafka_mode(&self) -> error_stack::Result<KafkaIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(IngestDeliveryKind::AckSequential) => Ok(KafkaIngestMode::AckSequential {
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(IngestDeliveryKind::AckParallel) => Ok(KafkaIngestMode::AckParallel {
                max: self.ack_max()?,
                batch_timeout: self
                    .required(&self.batch_timeout, IngestorEntityDraftError::BatchTimeout)?,
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(IngestDeliveryKind::NoAck) => Ok(KafkaIngestMode::NoAck),
            Some(IngestDeliveryKind::AckRequest) | None => {
                Err(Report::new(IngestorEntityDraftError::Mode))
            }
        }
    }

    fn pulsar_mode(&self) -> error_stack::Result<PulsarIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(IngestDeliveryKind::AckSequential) => Ok(PulsarIngestMode::AckSequential {
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(IngestDeliveryKind::AckParallel) => Ok(PulsarIngestMode::AckParallel {
                max: self.ack_max()?,
                batch_timeout: self
                    .required(&self.batch_timeout, IngestorEntityDraftError::BatchTimeout)?,
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(IngestDeliveryKind::NoAck) => Ok(PulsarIngestMode::NoAck),
            Some(IngestDeliveryKind::AckRequest) | None => {
                Err(Report::new(IngestorEntityDraftError::Mode))
            }
        }
    }

    fn endpoint_mode(&self) -> error_stack::Result<EndpointIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(IngestDeliveryKind::NoAck) => Ok(EndpointIngestMode::NoAck),
            Some(IngestDeliveryKind::AckRequest) => {
                let max = self
                    .ack_max
                    .trim()
                    .parse::<NonZeroU16>()
                    .change_context(IngestorEntityDraftError::AckMax)?;
                Ok(EndpointIngestMode::Ack {
                    max,
                    timeout: self.ack_timeout()?,
                })
            }
            Some(IngestDeliveryKind::AckSequential | IngestDeliveryKind::AckParallel) | None => {
                Err(Report::new(IngestorEntityDraftError::Mode))
            }
        }
    }

    fn max_body_size(
        &self,
    ) -> error_stack::Result<Option<PayloadSizeLimit>, IngestorEntityDraftError> {
        if self.max_body_size.trim().is_empty() {
            return Ok(None);
        }
        let limit = self
            .max_body_size
            .trim()
            .parse::<PayloadSizeLimit>()
            .change_context(IngestorEntityDraftError::MaxBodySize)?;
        Ok(Some(limit))
    }

    fn build_upgrade_headers(
        &self,
    ) -> error_stack::Result<Vec<WebsocketsUpgradeHeader>, IngestorEntityDraftError> {
        WebsocketsUpgradeHeaders::parse(&self.upgrade_headers)
            .change_context(IngestorEntityDraftError::UpgradeHeaders)?;
        Ok(self.upgrade_headers.clone())
    }

    fn rabbitmq_mode(&self) -> error_stack::Result<RabbitMqIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(IngestDeliveryKind::NoAck) => Ok(RabbitMqIngestMode::NoAck),
            Some(IngestDeliveryKind::AckSequential) => Ok(RabbitMqIngestMode::AckSequential {
                timeout: self.ack_timeout()?,
                retry_policy: self.retry_policy()?,
            }),
            Some(IngestDeliveryKind::AckParallel) => {
                let max = self
                    .ack_max
                    .trim()
                    .parse::<NonZeroU16>()
                    .change_context(IngestorEntityDraftError::AckMax)?;
                Ok(RabbitMqIngestMode::AckParallel {
                    max,
                    batch_timeout: self
                        .required(&self.batch_timeout, IngestorEntityDraftError::BatchTimeout)?,
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                })
            }
            Some(IngestDeliveryKind::AckRequest) | None => {
                Err(Report::new(IngestorEntityDraftError::Mode))
            }
        }
    }

    /// The session the draft chooses, which every MQTT TOPIC statement writes.
    fn mqtt_session(&self) -> error_stack::Result<MqttSession, IngestorEntityDraftError> {
        self.mqtt_session
            .ok_or_else(|| Report::new(IngestorEntityDraftError::MqttSession))
    }

    /// The quality of service the draft chooses, which every MQTT TOPIC statement writes.
    fn mqtt_qos(&self) -> error_stack::Result<MqttQos, IngestorEntityDraftError> {
        self.mqtt_qos
            .ok_or_else(|| Report::new(IngestorEntityDraftError::MqttQos))
    }

    fn mqtt_mode(&self) -> error_stack::Result<MqttIngestMode, IngestorEntityDraftError> {
        match self.delivery {
            Some(IngestDeliveryKind::NoAck) => Ok(MqttIngestMode::NoAck),
            Some(IngestDeliveryKind::AckSequential | IngestDeliveryKind::AckParallel) => {
                if self.delivery == Some(IngestDeliveryKind::AckSequential) {
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
            Some(IngestDeliveryKind::AckRequest) | None => {
                Err(Report::new(IngestorEntityDraftError::Mode))
            }
        }
    }

    fn build_quiesce(&self) -> error_stack::Result<IngestQuiesceMode, IngestorEntityDraftError> {
        let choice = self
            .quiesce
            .ok_or_else(|| Report::new(IngestorEntityDraftError::Quiesce))?;
        if !self.quiesce_enabled(choice) {
            return Err(Report::new(IngestorEntityDraftError::UnsupportedQuiesce));
        }
        match choice {
            IngestQuiesceKind::Suspend => Ok(IngestQuiesceMode::Suspend),
            IngestQuiesceKind::Drop => Ok(IngestQuiesceMode::Drop),
            IngestQuiesceKind::Reject => Ok(IngestQuiesceMode::Reject {
                retry_after: self
                    .required(&self.retry_after, IngestorEntityDraftError::RetryAfter)?,
            }),
            IngestQuiesceKind::EndpointBuffer => Ok(IngestQuiesceMode::EndpointBuffer {
                max_size: self.required(&self.buffer_size, IngestorEntityDraftError::BufferSize)?,
            }),
            IngestQuiesceKind::Buffer => Ok(IngestQuiesceMode::Buffer {
                max_size: self.required(&self.buffer_size, IngestorEntityDraftError::BufferSize)?,
                overflow: self
                    .overflow
                    .ok_or_else(|| Report::new(IngestorEntityDraftError::Overflow))?,
            }),
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
    #[error("Enter an HTTP request method")]
    Method,
    #[error("MAX BODY SIZE must be a positive supported byte size")]
    MaxBodySize,
    #[error("Enter valid literal upgrade headers")]
    UpgradeHeaders,
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
    #[error("ACK MAX must be a positive integer")]
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
    #[error("The selected mode requires {}", requirement.phrase())]
    OptionRequirement { requirement: ChoiceRequirement },
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
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{
        CodecName, CreateStatement, GeneralErrorPolicy, HttpIngestorEntity, IngestDeliveryKind,
        IngestQuiesceKind, IngestQuiesceOverflow, IngestorEntity, IngestorEntityKind,
        KafkaIngestorEntity, Model, ModelKind, ModelName, MqttIngestMode, MqttIngestor,
        MqttIngestorEntity, MqttQos, MqttSession, NatsIngestorEntity, NodeRef, PulsarIngestor,
        PulsarIngestorEntity, PulsarSubscriptionOwnership, RedisIngestorEntity,
        RequestedResourceVersion, Statement, WebsocketsIngestorEntity,
    };
    use nervix_plexus::statement::parse_statement;

    use super::{IngestorEntityDraft, IngestorEntityDraftError, OffsetChoice};
    use crate::create_dialog::{
        SelectedReference,
        ingestor_draft::{IngestorDraft, TimestampDraft},
        ingestor_route_draft::{FlushDraft, InheritDraft, MessageErrorDraft},
    };

    #[test]
    fn offered_modes_follow_each_entity_declaration_in_declared_order() {
        for kind in IngestorEntityKind::modeled() {
            let mut draft = IngestorEntityDraft::default();
            draft.choose_kind(kind);
            let offered = draft
                .available_modes()
                .iter()
                .map(|mode| mode.phrase())
                .collect::<Vec<_>>();
            let declared = kind
                .capabilities()
                .delivery
                .iter()
                .map(|delivery| delivery.kind.phrase())
                .collect::<Vec<_>>();
            assert_eq!(offered, declared, "{}", kind.phrase());
        }
    }

    #[test]
    fn declared_option_choices_bind_to_the_selected_semantic_value() {
        for kind in IngestorEntityKind::modeled() {
            let mut draft = IngestorEntityDraft::default();
            draft.choose_kind(kind);
            for option in kind.capabilities().options {
                if let nervix_models::OptionForm::Choice(choices) = option.form {
                    for choice in choices {
                        draft.choose_option(option.keywords, choice.name);
                        assert_eq!(
                            draft.chosen_option(option.keywords),
                            Some(choice.name),
                            "{} {}",
                            kind.phrase(),
                            option.keywords
                        );
                    }
                }
            }
        }
    }

    fn filled_from(source: &IngestorEntity) -> IngestorEntityDraft {
        use nervix_models::*;
        let kind = source.kind();
        let mut draft = IngestorEntityDraft::default();
        draft.choose_kind(kind);
        draft.select_reference(&NodeRef::new(
            kind.capabilities().reference.model_kind(),
            source.reference().name().clone(),
        ));
        draft.delivery = Some(source.acknowledgement().kind());
        match source.acknowledgement() {
            IngestAcknowledgement::Unacknowledged => {}
            IngestAcknowledgement::Request { max, timeout } => {
                draft.ack_max = max.to_string();
                draft.ack_timeout = timeout.to_owned();
            }
            IngestAcknowledgement::Sequential { timeout, retry } => {
                draft.ack_timeout = timeout.to_owned();
                draft.retry_backoff = retry.backoff.clone();
                draft.retry_max = retry.max_backoff.clone();
            }
            IngestAcknowledgement::Parallel {
                max,
                batch_timeout,
                timeout,
                retry,
            } => {
                draft.ack_max = max.to_string();
                draft.batch_timeout = batch_timeout.to_owned();
                draft.ack_timeout = timeout.to_owned();
                draft.retry_backoff = retry.backoff.clone();
                draft.retry_max = retry.max_backoff.clone();
            }
        }
        draft.quiesce = Some(source.quiesce().kind());
        match source.quiesce() {
            IngestQuiesceMode::Suspend | IngestQuiesceMode::Drop => {}
            IngestQuiesceMode::Reject { retry_after } => draft.retry_after = retry_after.clone(),
            IngestQuiesceMode::EndpointBuffer { max_size } => draft.buffer_size = max_size.clone(),
            IngestQuiesceMode::Buffer { max_size, overflow } => {
                draft.buffer_size = max_size.clone();
                draft.overflow = Some(*overflow);
            }
        }
        match source {
            IngestorEntity::Http(HttpIngestor::Request(entity)) => {
                draft.method = entity.method.clone();
                draft.request_headers = entity.headers.clone();
                draft.max_body_size = entity
                    .max_body_size
                    .map(|limit| limit.to_string())
                    .unwrap_or_default();
                draft.every = entity.every.to_string();
            }
            IngestorEntity::Websockets(WebsocketsIngestor::Connection(entity)) => {
                draft.upgrade_headers = entity.headers.clone();
            }
            IngestorEntity::Http(HttpIngestor::Endpoint(_))
            | IngestorEntity::Websockets(WebsocketsIngestor::Endpoint(_))
            | IngestorEntity::ZeroMq(_)
            | IngestorEntity::Syslog(_) => {}
            IngestorEntity::Kafka(KafkaIngestor::Topic(entity)) => {
                draft.topic = entity.topic.to_string();
                draft.instances = entity.instances.to_string();
                match &entity.offset_mode {
                    KafkaOffsetMode::Domain => draft.offset = Some(OffsetChoice::Domain),
                    KafkaOffsetMode::ConsumerGroup(group) => {
                        draft.offset = Some(OffsetChoice::ConsumerGroup);
                        draft.consumer_group = group.to_string();
                    }
                }
            }
            IngestorEntity::Pulsar(PulsarIngestor::Topic(entity)) => {
                draft.topic = entity.topic.to_string();
                draft.instances = entity.instances.to_string();
                draft.subscription = entity.subscription.name.to_string();
                draft.subscription_ownership = Some(entity.subscription.ownership);
            }
            IngestorEntity::Mqtt(MqttIngestor::Topic(entity)) => {
                draft.topic = entity.topic.to_string();
                draft.instances = entity.instances.to_string();
                draft.mqtt_session = Some(entity.session);
                draft.mqtt_qos = Some(entity.qos);
            }
            IngestorEntity::Nats(NatsIngestor::Subject(entity)) => {
                draft.subject = entity.subject.to_string();
                draft.queue_group = entity.queue_group.to_string();
                draft.instances = entity.instances.to_string();
            }
            IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(entity)) => {
                draft.queue = entity.queue.to_string();
                draft.instances = entity.instances.to_string();
            }
            IngestorEntity::Redis(RedisIngestor::PubSub(entity)) => {
                draft.channel = entity.channel.to_string()
            }
            IngestorEntity::Sqs(SqsIngestor::Queue(entity)) => {
                draft.queue = entity.queue.to_string();
                draft.instances = entity.instances.to_string();
            }
            IngestorEntity::Prometheus(PrometheusIngestor::Query(entity)) => {
                draft.query = entity.query.clone();
                draft.max_body_size = match entity.max_body_size {
                    Some(limit) => limit.to_string(),
                    None => String::new(),
                };
                draft.every = entity.every.to_string();
            }
        }
        draft
    }

    #[test]
    fn bolero_ingestor_source_drafts_preserve_every_declared_entity_configuration() {
        use nervix_arbitrary::{Arbitrary, Domain};
        bolero::check!()
            .with_iterations(128)
            .with_max_len(2048)
            .for_each(|bytes: &[u8]| {
                for kind in IngestorEntityKind::modeled() {
                    let mut arbitrary = Arbitrary::new(bytes, Domain::Plexus);
                    let mut source = arbitrary
                        .ingest_source_of(kind)
                        .assured("a modeled source has a transport");
                    // A completed form has required query text with no surrounding whitespace.
                    // Its interior still spans arbitrary text, including quotes and Unicode.
                    if let nervix_models::IngestorEntity::Prometheus(
                        nervix_models::PrometheusIngestor::Query(entity),
                    ) = &mut source
                    {
                        entity.query = format!("query({})", entity.query);
                    }
                    let draft = filled_from(&source);
                    assert_eq!(
                        draft
                            .build()
                            .assured("every generated declaration-valid source is editable"),
                        source
                    );
                }
            });
    }

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
        draft.method = "GET".into();
        draft.every = "1s".into();
        draft.instances = "1".into();
        draft.offset = Some(OffsetChoice::Domain);
        draft.delivery = Some(match kind {
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic)
            | IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic)
            | IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => IngestDeliveryKind::NoAck,
            _ => kind.capabilities().delivery[0].kind,
        });
        draft.ack_max = "2".into();
        draft.batch_timeout = "1s".into();
        draft.ack_timeout = "30s".into();
        draft.retry_backoff = "200ms".into();
        draft.retry_max = "5s".into();
        draft.mqtt_session = Some(MqttSession::Clean);
        draft.mqtt_qos = Some(MqttQos::Qos0);
        draft.quiesce = Some(match kind {
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint)
            | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                IngestQuiesceKind::Reject
            }
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject)
            | IngestorEntityKind::Redis(RedisIngestorEntity::PubSub)
            | IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
                IngestQuiesceKind::Drop
            }
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => IngestQuiesceKind::Buffer,
            _ => IngestQuiesceKind::Suspend,
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
        draft.delivery = Some(IngestDeliveryKind::AckParallel);
        draft.quiesce = Some(IngestQuiesceKind::Suspend);
        let Err(refused) = draft.build() else {
            panic!("an acknowledged mode needs a persistent session at QoS 1 or 2");
        };
        assert_eq!(
            refused.current_context().to_string(),
            "The selected mode requires SESSION PERSISTENT"
        );
        draft.mqtt_session = Some(MqttSession::Persistent);
        let Err(refused) = draft.build() else {
            panic!("an acknowledged mode needs QoS 1 or 2");
        };
        assert_eq!(
            refused.current_context().to_string(),
            "The selected mode requires QOS 1 or 2"
        );
        for qos in [MqttQos::Qos1, MqttQos::Qos2] {
            draft.mqtt_qos = Some(qos);
            assert!(draft.build().is_ok(), "{qos:?}");
        }
        draft.delivery = Some(IngestDeliveryKind::NoAck);
        draft.mqtt_session = Some(MqttSession::Clean);
        assert!(draft.build().is_err());
    }

    #[test]
    fn an_mqtt_draft_writes_the_session_and_quality_of_service_it_chose() {
        let mut draft = complete(IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic));
        draft.mqtt_session = None;
        let Err(refused) = draft.build() else {
            panic!("every MQTT TOPIC statement writes its session");
        };
        assert!(matches!(
            refused.current_context(),
            IngestorEntityDraftError::MqttSession
        ));
        draft.mqtt_session = Some(MqttSession::Clean);
        draft.mqtt_qos = None;
        let Err(refused) = draft.build() else {
            panic!("every MQTT TOPIC statement writes its quality of service");
        };
        assert!(matches!(
            refused.current_context(),
            IngestorEntityDraftError::MqttQos
        ));
        draft.mqtt_qos = Some(MqttQos::Qos2);
        let Ok(IngestorEntity::Mqtt(MqttIngestor::Topic(entity))) = draft.build() else {
            panic!("a complete MQTT draft builds its topic source");
        };
        assert_eq!(entity.session, MqttSession::Clean);
        assert_eq!(entity.qos, MqttQos::Qos2);
        assert_eq!(entity.mode, MqttIngestMode::NoAck);
    }

    #[test]
    fn every_source_quiesce_choice_builds_only_when_its_contract_is_met() {
        for kind in IngestorEntityKind::modeled() {
            let draft = complete(kind);
            for choice in draft.available_quiesce().iter() {
                let mut candidate = draft.clone();
                candidate.quiesce = Some(*choice);
                if kind == IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic)
                    && *choice == IngestQuiesceKind::Suspend
                {
                    candidate.mqtt_session = Some(MqttSession::Persistent);
                    candidate.mqtt_qos = Some(MqttQos::Qos1);
                }
                assert!(
                    candidate.build().is_ok(),
                    "{} {}",
                    kind.form_label(),
                    choice.keyword()
                );
            }
        }
        let mut kafka = complete(IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic));
        kafka.quiesce = Some(IngestQuiesceKind::Buffer);
        assert!(kafka.build().is_err());
        let mut endpoint = complete(IngestorEntityKind::Http(HttpIngestorEntity::Endpoint));
        endpoint.quiesce = Some(IngestQuiesceKind::Drop);
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
