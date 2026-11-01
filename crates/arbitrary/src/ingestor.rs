//! Ingestors: the transport or client source they read, and the routes that build their records.

use std::num::{NonZeroU16, NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, ClientIngestMode, ClientIngestorEntity, ClientProducerIngestor, CreateIngestor,
    EndpointIngestMode, HttpEndpointIngestor, HttpIngestorEntity, HttpRequestIngestMode,
    HttpRequestIngestor, IngestQuiesceMode, IngestQuiesceOverflow, IngestTimestampSource,
    IngestorDeclaration, IngestorEntity, IngestorEntityBody, IngestorEntityKind, IngestorInput,
    KafkaIngestMode, KafkaIngestorEntity, KafkaOffsetMode, KafkaTopicIngestor, MqttIngestMode,
    MqttIngestorEntity, MqttQos, MqttSession, MqttTopicIngestor, NatsIngestMode,
    NatsIngestorEntity, NatsSubjectIngestor, PrometheusIngestMode, PrometheusIngestorEntity,
    PrometheusQueryIngestor, PulsarIngestMode, PulsarIngestorEntity, PulsarSubscription,
    PulsarSubscriptionOwnership, PulsarTopicIngestor, RabbitMqIngestMode, RabbitMqIngestorEntity,
    RabbitMqQueueIngestor, RedisIngestorEntity, RedisPubSubIngestMode, RedisPubSubIngestor,
    RetryPolicy, SqsIngestMode, SqsIngestorEntity, SqsQueueIngestor, SyslogIngestMode,
    SyslogIngestorEntity, SyslogListenerIngestor, TransportIngestorInput,
    WebsocketsConnectionIngestor, WebsocketsEndpointIngestor, WebsocketsIngestMode,
    WebsocketsIngestorEntity, ZeroMqIngestMode, ZeroMqIngestorEntity, ZeroMqPullIngestor,
};

use crate::{
    Arbitrary,
    route::{RouteBranch, RouteFlush, RouteShape},
};

impl Arbitrary<'_> {
    /// An ingestor over a source of any connector and entity, whose every route constructs its
    /// own outgoing branch.
    pub fn create_ingestor(&mut self) -> CreateIngestor {
        let kind = self.ingest_source_kind();
        self.create_ingestor_from(kind)
    }

    /// Any connector and entity an ingestor reads.
    pub fn ingest_source_kind(&mut self) -> IngestorEntityKind {
        let kinds = IngestorEntityKind::all().collect::<Vec<_>>();
        let count = NonZeroUsize::new(kinds.len()).assured("the vocabulary declares sources");
        kinds[self.entropy.index(count)]
    }

    /// An ingestor over a source of the requested connector and entity.
    pub fn create_ingestor_from(&mut self, kind: IngestorEntityKind) -> CreateIngestor {
        let input = self.ingestor_input_of(kind);
        let timestamp_source = match self.entropy.byte() % 4 {
            0 => None,
            1 => Some(IngestTimestampSource::Now),
            2 => Some(IngestTimestampSource::At(self.name())),
            _ => Some(IngestTimestampSource::AtMetadata(self.name())),
        };
        CreateIngestor {
            name: self.name(),
            output_routes: self.processor_outputs(
                RouteShape::Transforming,
                RouteFlush::Required,
                RouteBranch::PerRoute,
            ),
            input,
            timestamp_source,
            general_error_policy: self.general_error_policy(),
            filter_where: self.optional_expression(),
        }
    }

    /// The input of the requested connector and entity: a transport with the codec that decodes
    /// it, or the batches session producers submit.
    pub fn ingestor_input_of(&mut self, kind: IngestorEntityKind) -> IngestorInput {
        match self.ingest_source_of(kind) {
            Some(source) => IngestorInput::Transport(TransportIngestorInput {
                source,
                codec: self.name(),
            }),
            None => IngestorInput::Client(self.client_producer_source()),
        }
    }

    /// The batches session producers submit into a client ingestor.
    pub fn client_producer_source(&mut self) -> ClientProducerIngestor {
        ClientProducerIngestor {
            schema: self.name(),
            mode: ClientIngestMode {
                window: self.ack_window(),
                ack_timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
        }
    }

    /// A transport source of any connector and entity, quiescing in a mode it supports.
    pub fn ingest_source(&mut self) -> IngestorEntity {
        let kinds = IngestorEntityKind::modeled().collect::<Vec<_>>();
        let count =
            NonZeroUsize::new(kinds.len()).assured("the vocabulary declares transport sources");
        let kind = kinds[self.entropy.index(count)];
        self.ingest_source_of(kind)
            .verified("the kind was chosen among those the transport union holds a Model of")
    }

    /// The transport source of the requested connector and entity, quiescing in a mode it
    /// supports. A client producer is no transport, so it has none.
    ///
    /// The match is exhaustive, so a new pair does not compile until the generator is taught to
    /// build it.
    pub fn ingest_source_of(&mut self, kind: IngestorEntityKind) -> Option<IngestorEntity> {
        let source = match kind {
            IngestorEntityKind::Client(ClientIngestorEntity::Producer) => return None,
            IngestorEntityKind::Http(HttpIngestorEntity::Request) => {
                self.http_request_source().into()
            }
            IngestorEntityKind::Http(HttpIngestorEntity::Endpoint) => {
                self.http_endpoint_source().into()
            }
            IngestorEntityKind::Kafka(KafkaIngestorEntity::Topic) => {
                self.kafka_topic_source().into()
            }
            IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic) => {
                self.pulsar_topic_source().into()
            }
            IngestorEntityKind::Mqtt(MqttIngestorEntity::Topic) => self.mqtt_topic_source().into(),
            IngestorEntityKind::Nats(NatsIngestorEntity::Subject) => {
                self.nats_subject_source().into()
            }
            IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue) => {
                self.rabbitmq_queue_source().into()
            }
            IngestorEntityKind::Redis(RedisIngestorEntity::PubSub) => {
                self.redis_pubsub_source().into()
            }
            IngestorEntityKind::Prometheus(PrometheusIngestorEntity::Query) => {
                self.prometheus_query_source().into()
            }
            IngestorEntityKind::ZeroMq(ZeroMqIngestorEntity::Pull) => {
                self.zeromq_pull_source().into()
            }
            IngestorEntityKind::Sqs(SqsIngestorEntity::Queue) => self.sqs_queue_source().into(),
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Connection) => {
                self.websockets_connection_source().into()
            }
            IngestorEntityKind::Websockets(WebsocketsIngestorEntity::Endpoint) => {
                self.websockets_endpoint_source().into()
            }
            IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener) => {
                self.syslog_listener_source().into()
            }
        };
        Some(source)
    }

    /// An HTTP request polled on a cadence of domain time.
    pub fn http_request_source(&mut self) -> HttpRequestIngestor {
        let method =
            ["GET", "HEAD", "POST", "PATCH"][usize::from(self.entropy().byte() % 4)].to_owned();
        let mut headers = Vec::new();
        for index in 0..(self.entropy().byte() % 3) {
            let value = if self.entropy.flag() {
                nervix_models::HttpRequestHeaderValue::ClientSecret(format!(
                    "{}fixture-{}'\"\\",
                    nervix_models::HTTP_CLIENT_SECRET_KEY_PREFIX,
                    self.entropy().byte()
                ))
            } else {
                nervix_models::HttpRequestHeaderValue::Literal(format!(
                    "fixture-{}'\"\\",
                    self.entropy().byte()
                ))
            };
            headers.push(nervix_models::HttpRequestHeader {
                name: format!("x-nervix-event-{index}"),
                value,
            });
        }
        let max_body_size = if self.entropy.flag() {
            Some(self.payload_size_limit())
        } else {
            None
        };
        let source = HttpRequestIngestor {
            client: self.name(),
            method,
            headers,
            max_body_size,
            every: self.clock_period(),
            mode: HttpRequestIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// An HTTP endpoint the cluster serves.
    pub fn http_endpoint_source(&mut self) -> HttpEndpointIngestor {
        let source = HttpEndpointIngestor {
            endpoint: self.name(),
            mode: self.endpoint_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A Kafka topic, its offsets kept by a consumer group or by the domain.
    pub fn kafka_topic_source(&mut self) -> KafkaTopicIngestor {
        let offset_mode = if self.entropy.flag() {
            KafkaOffsetMode::ConsumerGroup(self.kafka_consumer_group_name())
        } else {
            KafkaOffsetMode::Domain
        };
        let source = KafkaTopicIngestor {
            client: self.name(),
            topic: self.kafka_topic_name(),
            offset_mode,
            instances: self.positive_u64(),
            mode: self.kafka_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A Pulsar topic read through a subscription that the ingestor owns or an operator
    /// provisioned.
    pub fn pulsar_topic_source(&mut self) -> PulsarTopicIngestor {
        let client = self.name();
        let topic = self.pulsar_topic_name();
        let name = self.pulsar_subscription_name();
        let ownership = if self.entropy.flag() {
            PulsarSubscriptionOwnership::Ingestor
        } else {
            PulsarSubscriptionOwnership::Provisioned
        };
        let source = PulsarTopicIngestor {
            client,
            topic,
            subscription: PulsarSubscription { name, ownership },
            instances: self.positive_u64(),
            mode: self.pulsar_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// An MQTT topic filter.
    pub fn mqtt_topic_source(&mut self) -> MqttTopicIngestor {
        let source = MqttTopicIngestor {
            client: self.name(),
            topic: self.mqtt_topic_filter(),
            instances: self.positive_u64(),
            mode: self.mqtt_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A NATS subject read as a member of a queue group.
    pub fn nats_subject_source(&mut self) -> NatsSubjectIngestor {
        let source = NatsSubjectIngestor {
            client: self.name(),
            subject: self.nats_subject_name(),
            queue_group: self.nats_queue_group_name(),
            instances: self.positive_u64(),
            mode: NatsIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A RabbitMQ queue.
    pub fn rabbitmq_queue_source(&mut self) -> RabbitMqQueueIngestor {
        let source = RabbitMqQueueIngestor {
            client: self.name(),
            queue: self.rabbitmq_queue_name(),
            instances: self.positive_u64(),
            mode: self.rabbitmq_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A Redis Pub/Sub channel.
    pub fn redis_pubsub_source(&mut self) -> RedisPubSubIngestor {
        let source = RedisPubSubIngestor {
            client: self.name(),
            channel: self.redis_channel_name(),
            mode: RedisPubSubIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A PromQL query evaluated on a cadence of domain time, with or without its response bound.
    pub fn prometheus_query_source(&mut self) -> PrometheusQueryIngestor {
        let client = self.name();
        let query = self.string();
        let max_body_size = if self.entropy.flag() {
            Some(self.payload_size_limit())
        } else {
            None
        };
        let source = PrometheusQueryIngestor {
            client,
            query,
            max_body_size,
            every: self.clock_period(),
            mode: PrometheusIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A ZeroMQ pull socket.
    pub fn zeromq_pull_source(&mut self) -> ZeroMqPullIngestor {
        let source = ZeroMqPullIngestor {
            client: self.name(),
            mode: ZeroMqIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// An SQS queue.
    pub fn sqs_queue_source(&mut self) -> SqsQueueIngestor {
        let source = SqsQueueIngestor {
            client: self.name(),
            queue: self.sqs_queue_name(),
            instances: self.positive_u64(),
            mode: SqsIngestMode::AckSequential {
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// The connection a WebSockets client opens.
    pub fn websockets_connection_source(&mut self) -> WebsocketsConnectionIngestor {
        let mut headers = Vec::new();
        for index in 0..(self.entropy().byte() % 3) {
            headers.push(nervix_models::WebsocketsUpgradeHeader {
                name: format!("x-nervix-event-{index}"),
                value: format!("fixture-{}'\"\\", self.entropy().byte()),
            });
        }
        let source = WebsocketsConnectionIngestor {
            client: self.name(),
            headers,
            mode: WebsocketsIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// A WebSockets endpoint the cluster serves.
    pub fn websockets_endpoint_source(&mut self) -> WebsocketsEndpointIngestor {
        let source = WebsocketsEndpointIngestor {
            endpoint: self.name(),
            mode: self.endpoint_mode(),
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// The listener a syslog client binds.
    pub fn syslog_listener_source(&mut self) -> SyslogListenerIngestor {
        let source = SyslogListenerIngestor {
            client: self.name(),
            mode: SyslogIngestMode::NoAck,
            quiesce: IngestQuiesceMode::Suspend,
        };
        self.quiescing_as_supported(source)
    }

    /// `source` quiescing in one of the modes it supports.
    fn quiescing_as_supported<Source: IngestorEntityBody + IngestorDeclaration>(
        &mut self,
        mut source: Source,
    ) -> Source {
        let quiesce = self.supported_quiesce(&source);
        *source.quiesce_mut() = quiesce;
        source
    }

    /// Any quiesce mode a statement can write.
    pub fn quiesce_mode(&mut self) -> IngestQuiesceMode {
        let candidates = self.quiesce_candidates();
        let count = NonZeroUsize::new(candidates.len()).assured("five quiesce modes exist");
        let chosen = self.entropy.index(count);
        candidates[chosen].clone()
    }

    /// One of the quiesce modes `source` supports, as its entity declares them.
    fn supported_quiesce<Source: IngestorEntityBody + IngestorDeclaration>(
        &mut self,
        source: &Source,
    ) -> IngestQuiesceMode {
        let candidates = self.quiesce_candidates();
        let mut supported = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if source.supports_quiesce(&candidate) {
                supported.push(candidate);
            }
        }
        let count = NonZeroUsize::new(supported.len())
            .assured("every transport supports at least one quiesce mode");
        let chosen = self.entropy.index(count);
        supported.swap_remove(chosen)
    }

    /// Every quiesce mode, each with its own sizes and durations.
    fn quiesce_candidates(&mut self) -> [IngestQuiesceMode; 5] {
        [
            IngestQuiesceMode::Suspend,
            IngestQuiesceMode::Buffer {
                max_size: self.positive_byte_size(),
                overflow: self.entropy.pick([
                    IngestQuiesceOverflow::DropOldest,
                    IngestQuiesceOverflow::DropNewest,
                ]),
            },
            IngestQuiesceMode::Drop,
            IngestQuiesceMode::Reject {
                retry_after: self.duration(),
            },
            IngestQuiesceMode::EndpointBuffer {
                max_size: self.positive_byte_size(),
            },
        ]
    }

    fn endpoint_mode(&mut self) -> EndpointIngestMode {
        if self.entropy.flag() {
            let count = self.entropy.pick([1u16, 8, 256, u16::MAX]);
            EndpointIngestMode::Ack {
                max: NonZeroU16::new(count).assured("every declared window boundary is positive"),
                timeout: self.duration(),
            }
        } else {
            EndpointIngestMode::NoAck
        }
    }

    fn kafka_mode(&mut self) -> KafkaIngestMode {
        match self.entropy.byte() % 3 {
            0 => KafkaIngestMode::AckParallel {
                max: self.positive_u64(),
                batch_timeout: self.duration(),
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            1 => KafkaIngestMode::AckSequential {
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            _ => KafkaIngestMode::NoAck,
        }
    }

    fn pulsar_mode(&mut self) -> PulsarIngestMode {
        match self.entropy.byte() % 3 {
            0 => PulsarIngestMode::AckParallel {
                max: self.positive_u64(),
                batch_timeout: self.duration(),
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            1 => PulsarIngestMode::AckSequential {
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            _ => PulsarIngestMode::NoAck,
        }
    }

    fn rabbitmq_mode(&mut self) -> RabbitMqIngestMode {
        match self.entropy.byte() % 3 {
            0 => RabbitMqIngestMode::NoAck,
            1 => RabbitMqIngestMode::AckSequential {
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            _ => {
                let value = self.entropy.boundary_biased(1..=u64::from(u16::MAX));
                let count =
                    u16::try_from(value).verified("the sampled range fits AMQP's prefetch field");
                let max = NonZeroU16::new(count).verified("the sampled range starts at one");
                RabbitMqIngestMode::AckParallel {
                    max,
                    batch_timeout: self.duration(),
                    timeout: self.duration(),
                    retry_policy: self.retry_policy(),
                }
            }
        }
    }

    fn mqtt_mode(&mut self) -> MqttIngestMode {
        let session = self
            .entropy
            .pick([MqttSession::Clean, MqttSession::Persistent]);
        let qos = self
            .entropy
            .pick([MqttQos::AtMostOnce, MqttQos::AtLeastOnce]);
        match self.entropy.byte() % 3 {
            0 => MqttIngestMode::NoAck { session, qos },
            1 => MqttIngestMode::AckSequential {
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
            _ => MqttIngestMode::AckParallel {
                max: self.positive_u64(),
                batch_timeout: self.duration(),
                timeout: self.duration(),
                retry_policy: self.retry_policy(),
            },
        }
    }

    /// A retry policy: the first backoff and the most it grows to.
    pub fn retry_policy(&mut self) -> RetryPolicy {
        RetryPolicy {
            backoff: self.duration(),
            max_backoff: self.duration(),
        }
    }

    /// An acknowledgement window: one at a time, or up to a positive count in parallel.
    pub fn ack_window(&mut self) -> AckWindow {
        if self.entropy.flag() {
            AckWindow::Sequential
        } else {
            AckWindow::Parallel {
                max: self.positive_u64(),
            }
        }
    }
}
