//! Incomplete source settings for a visual ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** Source-specific fields, explicit delivery and quiesce selections, and their
//!   conversion to one current `IngestSource` variant.
//! - **Depends on.** Source vocabulary, typed names, and selected browser references.
//! - **Must not know.** Connector drivers, registry snapshots, or runtime quiescence.

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{
    ChannelName, ClientName, ConsumerGroupName, DomainClockPeriod, EndpointIngestMode,
    EndpointName, IngestQuiesceMode, IngestQuiesceOverflow, IngestSource, IngestSourceKind,
    KafkaIngestMode, KafkaOffsetMode, ModelKind, ModelName, MqttIngestMode, MqttQos, MqttSession,
    NatsIngestMode, NodeRef, PulsarSubscriptionName, QueueGroupName, QueueName, RabbitMqIngestMode,
    RedisPubSubIngestMode, RetryPolicy, SqsIngestMode, SubjectName, TopicName,
    WebsocketsIngestMode, ZeroMqIngestMode,
};
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryChoice {
    AckSequential,
    AckParallel,
    NoAckSequential,
    NoAckParallel,
}

impl DeliveryChoice {
    pub(super) const fn key(self) -> &'static str {
        match self {
            Self::AckSequential => "ack-sequential",
            Self::AckParallel => "ack-parallel",
            Self::NoAckSequential => "no-ack-sequential",
            Self::NoAckParallel => "no-ack-parallel",
        }
    }

    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::AckSequential => "ACK SEQUENTIAL",
            Self::AckParallel => "ACK PARALLEL",
            Self::NoAckSequential => "NO_ACK SEQUENTIAL",
            Self::NoAckParallel => "NO_ACK PARALLEL",
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
        let expected = if kind == IngestSourceKind::Endpoint {
            ModelKind::Endpoint
        } else {
            ModelKind::Client
        };
        if node.kind == expected {
            self.reference = Some(SelectedReference::chosen(node.identifier.clone()));
        }
    }

    pub(super) fn selects_reference(&self, node: &NodeRef) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        let expected = if kind == IngestSourceKind::Endpoint {
            ModelKind::Endpoint
        } else {
            ModelKind::Client
        };
        self.reference.as_ref().is_some_and(|reference| {
            reference.is_current() && node.kind == expected && reference.name() == &node.identifier
        })
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(reference) = &mut self.reference {
            reference.invalidate();
        }
    }

    pub(super) fn current_reference(&self) -> Option<&ModelName> {
        self.reference
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn available_modes(&self) -> &'static [DeliveryChoice] {
        match self.kind {
            Some(IngestSourceKind::Kafka | IngestSourceKind::Pulsar) => &[
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
                DeliveryChoice::NoAckParallel,
            ],
            Some(IngestSourceKind::Mqtt) => &[
                DeliveryChoice::NoAckSequential,
                DeliveryChoice::NoAckParallel,
                DeliveryChoice::AckSequential,
                DeliveryChoice::AckParallel,
            ],
            Some(IngestSourceKind::RabbitMq | IngestSourceKind::Sqs) => {
                &[DeliveryChoice::AckSequential]
            }
            Some(
                IngestSourceKind::Nats
                | IngestSourceKind::RedisPubSub
                | IngestSourceKind::ZeroMq
                | IngestSourceKind::Endpoint
                | IngestSourceKind::Websockets
                | IngestSourceKind::Syslog,
            ) => &[DeliveryChoice::NoAckSequential],
            Some(IngestSourceKind::Http | IngestSourceKind::Prometheus) | None => &[],
        }
    }

    pub(super) fn available_quiesce(&self) -> &'static [QuiesceChoice] {
        match self.kind {
            Some(
                IngestSourceKind::Kafka
                | IngestSourceKind::Pulsar
                | IngestSourceKind::RabbitMq
                | IngestSourceKind::Sqs,
            ) => &[QuiesceChoice::Suspend],
            Some(IngestSourceKind::Mqtt) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(
                IngestSourceKind::Nats
                | IngestSourceKind::RedisPubSub
                | IngestSourceKind::Websockets,
            ) => &[QuiesceChoice::Buffer, QuiesceChoice::Drop],
            Some(IngestSourceKind::ZeroMq | IngestSourceKind::Syslog) => &[
                QuiesceChoice::Suspend,
                QuiesceChoice::Buffer,
                QuiesceChoice::Drop,
            ],
            Some(IngestSourceKind::Http | IngestSourceKind::Prometheus) => {
                &[QuiesceChoice::Suspend, QuiesceChoice::Buffer]
            }
            Some(IngestSourceKind::Endpoint) => &[QuiesceChoice::Reject, QuiesceChoice::Buffer],
            None => &[],
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<IngestSource, IngestSourceDraftError> {
        let kind = self
            .kind
            .ok_or_else(|| Report::new(IngestSourceDraftError::Kind))?;
        if self.current_reference().is_none() {
            return Err(Report::new(if self.reference.is_some() {
                IngestSourceDraftError::ReferenceChanged
            } else {
                IngestSourceDraftError::Reference
            }));
        }
        let quiesce = self.build_quiesce(kind)?;
        let source = match kind {
            IngestSourceKind::Http => IngestSource::Http {
                client: self.client()?,
                every: self.every()?,
                quiesce,
            },
            IngestSourceKind::Kafka => IngestSource::Kafka {
                client: self.client()?,
                topic: TopicName::parse(self.topic.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Topic))?,
                offset_mode: self.offset_mode()?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            },
            IngestSourceKind::Pulsar => IngestSource::Pulsar {
                client: self.client()?,
                topic: TopicName::parse(self.topic.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Topic))?,
                subscription: PulsarSubscriptionName::parse(self.subscription.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Subscription))?,
                instances: self.instances()?,
                mode: self.kafka_mode()?,
                quiesce,
            },
            IngestSourceKind::Mqtt => IngestSource::Mqtt {
                client: self.client()?,
                topic: self.required(&self.topic, IngestSourceDraftError::Topic)?,
                instances: self.instances()?,
                mode: self.mqtt_mode()?,
                quiesce,
            },
            IngestSourceKind::Nats => IngestSource::Nats {
                client: self.client()?,
                subject: SubjectName::parse(self.subject.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Subject))?,
                queue_group: QueueGroupName::parse(self.queue_group.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::QueueGroup))?,
                instances: self.instances()?,
                mode: NatsIngestMode::NoAckSequential,
                quiesce,
            },
            IngestSourceKind::RabbitMq => IngestSource::RabbitMq {
                client: self.client()?,
                queue: QueueName::parse(self.queue.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Queue))?,
                instances: self.instances()?,
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            },
            IngestSourceKind::RedisPubSub => IngestSource::RedisPubSub {
                client: self.client()?,
                channel: ChannelName::parse(self.channel.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Channel))?,
                mode: RedisPubSubIngestMode::NoAckSequential,
                quiesce,
            },
            IngestSourceKind::Prometheus => IngestSource::Prometheus {
                client: self.client()?,
                query: self.required(&self.query, IngestSourceDraftError::Query)?,
                every: self.every()?,
                quiesce,
            },
            IngestSourceKind::ZeroMq => IngestSource::ZeroMq {
                client: self.client()?,
                mode: ZeroMqIngestMode::NoAckSequential,
                quiesce,
            },
            IngestSourceKind::Sqs => IngestSource::Sqs {
                client: self.client()?,
                queue: QueueName::parse(self.queue.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::Queue))?,
                instances: self.instances()?,
                mode: SqsIngestMode::AckSequential {
                    timeout: self.ack_timeout()?,
                    retry_policy: self.retry_policy()?,
                },
                quiesce,
            },
            IngestSourceKind::Endpoint => IngestSource::Endpoint {
                endpoint: self.endpoint()?,
                mode: EndpointIngestMode::NoAckSequential,
                quiesce,
            },
            IngestSourceKind::Websockets => IngestSource::Websockets {
                client: self.client()?,
                mode: WebsocketsIngestMode::NoAckSequential,
                quiesce,
            },
            IngestSourceKind::Syslog => IngestSource::Syslog {
                client: self.client()?,
                quiesce,
            },
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

    fn offset_mode(&self) -> error_stack::Result<KafkaOffsetMode, IngestSourceDraftError> {
        match self.offset {
            Some(OffsetChoice::Domain) => Ok(KafkaOffsetMode::Domain),
            Some(OffsetChoice::ConsumerGroup) => Ok(KafkaOffsetMode::ConsumerGroup(
                ConsumerGroupName::parse(self.consumer_group.trim())
                    .map_err(|_| Report::new(IngestSourceDraftError::ConsumerGroup))?,
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
            Some(DeliveryChoice::NoAckParallel) => Ok(KafkaIngestMode::NoAckParallel),
            Some(DeliveryChoice::NoAckSequential) | None => {
                Err(Report::new(IngestSourceDraftError::Mode))
            }
        }
    }

    fn mqtt_mode(&self) -> error_stack::Result<MqttIngestMode, IngestSourceDraftError> {
        match self.delivery {
            Some(DeliveryChoice::NoAckSequential | DeliveryChoice::NoAckParallel) => {
                let session = self
                    .mqtt_session
                    .ok_or_else(|| Report::new(IngestSourceDraftError::MqttSession))?;
                let qos = self
                    .mqtt_qos
                    .ok_or_else(|| Report::new(IngestSourceDraftError::MqttQos))?;
                if self.delivery == Some(DeliveryChoice::NoAckSequential) {
                    Ok(MqttIngestMode::NoAckSequential { session, qos })
                } else {
                    Ok(MqttIngestMode::NoAckParallel { session, qos })
                }
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

    fn build_quiesce(
        &self,
        kind: IngestSourceKind,
    ) -> error_stack::Result<IngestQuiesceMode, IngestSourceDraftError> {
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
                if kind == IngestSourceKind::Endpoint {
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
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CodecName, CreateStatement, GeneralErrorPolicy, IngestQuiesceOverflow, IngestSourceKind,
        Model, ModelKind, ModelName, MqttQos, MqttSession, NodeRef, RequestedResourceVersion,
        Statement,
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
        let reference_kind = if kind == IngestSourceKind::Endpoint {
            ModelKind::Endpoint
        } else {
            ModelKind::Client
        };
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
        draft.query = "up".into();
        draft.every = "1s".into();
        draft.instances = "1".into();
        draft.offset = Some(OffsetChoice::Domain);
        draft.delivery = Some(match kind {
            IngestSourceKind::Kafka | IngestSourceKind::Pulsar => DeliveryChoice::NoAckParallel,
            IngestSourceKind::Mqtt => DeliveryChoice::NoAckSequential,
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
            IngestSourceKind::Endpoint => QuiesceChoice::Reject,
            IngestSourceKind::Nats
            | IngestSourceKind::RedisPubSub
            | IngestSourceKind::Websockets => QuiesceChoice::Drop,
            IngestSourceKind::Mqtt => QuiesceChoice::Buffer,
            _ => QuiesceChoice::Suspend,
        });
        draft.buffer_size = "1MiB".into();
        draft.overflow = Some(IngestQuiesceOverflow::DropOldest);
        draft.retry_after = "5s".into();
        draft
    }

    #[test]
    fn every_offered_source_builds_its_current_variant_with_its_required_contract() {
        for kind in IngestSourceKind::ALL {
            let draft = complete(kind);
            assert_eq!(
                draft
                    .build()
                    .assured("complete source draft builds")
                    .transport_kind(),
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
        let mut draft = complete(IngestSourceKind::Mqtt);
        draft.delivery = Some(DeliveryChoice::AckParallel);
        draft.quiesce = Some(QuiesceChoice::Suspend);
        assert!(draft.build().is_err());
        draft.mqtt_session = Some(MqttSession::Persistent);
        draft.mqtt_qos = Some(MqttQos::AtLeastOnce);
        assert!(draft.build().is_ok());
        draft.delivery = Some(DeliveryChoice::NoAckSequential);
        draft.mqtt_session = Some(MqttSession::Clean);
        assert!(draft.build().is_err());
    }

    #[test]
    fn every_source_quiesce_choice_builds_only_when_its_contract_is_met() {
        for kind in IngestSourceKind::ALL {
            let draft = complete(kind);
            for choice in draft.available_quiesce() {
                let mut candidate = draft.clone();
                candidate.quiesce = Some(*choice);
                if kind == IngestSourceKind::Mqtt && *choice == QuiesceChoice::Suspend {
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
        let mut kafka = complete(IngestSourceKind::Kafka);
        kafka.quiesce = Some(QuiesceChoice::Buffer);
        assert!(kafka.build().is_err());
        let mut endpoint = complete(IngestSourceKind::Endpoint);
        endpoint.quiesce = Some(QuiesceChoice::Drop);
        assert!(endpoint.build().is_err());
    }

    #[test]
    fn source_switch_clears_fields_and_stale_reference_cannot_build() {
        let mut draft = complete(IngestSourceKind::Kafka);
        draft.invalidate_references();
        assert!(draft.build().is_err());
        draft.choose_kind(IngestSourceKind::Endpoint);
        assert!(draft.reference.is_none());
        assert!(draft.topic.is_empty());
        assert!(draft.quiesce.is_none());
    }
}
