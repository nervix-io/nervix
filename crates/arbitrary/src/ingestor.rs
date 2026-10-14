//! Ingestors: the transport or client source they read, and the routes that build their records.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, ClientIngestMode, ClientProducerSource, ClientSourceEntity, CreateIngestor,
    EndpointEndpointSource, EndpointIngestMode, EndpointSource, EndpointSourceEntity,
    HttpRequestSource, HttpSource, HttpSourceEntity, IngestQuiesceMode, IngestQuiesceOverflow,
    IngestSource, IngestSourceKind, IngestTimestampSource, IngestorInput, KafkaIngestMode,
    KafkaOffsetMode, KafkaSource, KafkaSourceEntity, KafkaTopicSource, MqttIngestMode, MqttQos,
    MqttSession, MqttSource, MqttSourceEntity, MqttTopicSource, NatsIngestMode, NatsSource,
    NatsSourceEntity, NatsSubjectSource, PrometheusQuerySource, PrometheusSource,
    PrometheusSourceEntity, PulsarSource, PulsarSourceEntity, PulsarTopicSource,
    RabbitMqIngestMode, RabbitMqQueueSource, RabbitMqSource, RabbitMqSourceEntity,
    RedisPubSubIngestMode, RedisPubSubSource, RedisSource, RedisSourceEntity, RetryPolicy,
    SqsIngestMode, SqsQueueSource, SqsSource, SqsSourceEntity, SyslogListenerSource, SyslogSource,
    SyslogSourceEntity, TransportIngestorInput, WebsocketsConnectionSource, WebsocketsIngestMode,
    WebsocketsSource, WebsocketsSourceEntity, ZeroMqIngestMode, ZeroMqPullSource, ZeroMqSource,
    ZeroMqSourceEntity,
};

use crate::{
    Arbitrary,
    route::{RouteBranch, RouteFlush, RouteShape},
};

impl Arbitrary<'_> {
    /// An ingestor over a source of any connector and entity, whose every route constructs its
    /// own outgoing branch.
    pub fn create_ingestor(&mut self) -> CreateIngestor {
        let kinds = IngestSourceKind::all().collect::<Vec<_>>();
        let count = NonZeroUsize::new(kinds.len()).assured("the vocabulary declares sources");
        let kind = kinds[self.entropy.index(count)];
        self.create_ingestor_from(kind)
    }

    /// An ingestor over a source of the requested connector and entity.
    pub fn create_ingestor_from(&mut self, kind: IngestSourceKind) -> CreateIngestor {
        let input = self.ingestor_input_of(kind);
        let timestamp_source = match self.entropy.byte() % 3 {
            0 => None,
            1 => Some(IngestTimestampSource::Now),
            _ => Some(IngestTimestampSource::At(self.name())),
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
    pub fn ingestor_input_of(&mut self, kind: IngestSourceKind) -> IngestorInput {
        match self.ingest_source_of(kind) {
            Some(source) => IngestorInput::Transport(TransportIngestorInput {
                source,
                codec: self.name(),
            }),
            None => IngestorInput::Client(ClientProducerSource {
                schema: self.name(),
                mode: ClientIngestMode {
                    window: self.ack_window(),
                    ack_timeout: self.duration(),
                    retry_policy: self.retry_policy(),
                },
            }),
        }
    }

    /// A transport source of any connector and entity, quiescing in a mode it supports.
    pub fn ingest_source(&mut self) -> IngestSource {
        let kinds = IngestSourceKind::modeled().collect::<Vec<_>>();
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
    pub fn ingest_source_of(&mut self, kind: IngestSourceKind) -> Option<IngestSource> {
        let mut source = match kind {
            IngestSourceKind::Client(ClientSourceEntity::Producer) => return None,
            IngestSourceKind::Http(HttpSourceEntity::Request) => {
                IngestSource::Http(HttpSource::Request(HttpRequestSource {
                    client: self.name(),
                    every: self.clock_period(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Kafka(KafkaSourceEntity::Topic) => {
                IngestSource::Kafka(KafkaSource::Topic(KafkaTopicSource {
                    client: self.name(),
                    topic: self.name(),
                    offset_mode: if self.entropy.flag() {
                        KafkaOffsetMode::ConsumerGroup(self.name())
                    } else {
                        KafkaOffsetMode::Domain
                    },
                    instances: self.positive_u64(),
                    mode: self.kafka_mode(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Pulsar(PulsarSourceEntity::Topic) => {
                IngestSource::Pulsar(PulsarSource::Topic(PulsarTopicSource {
                    client: self.name(),
                    topic: self.name(),
                    subscription: self.name(),
                    instances: self.positive_u64(),
                    mode: self.kafka_mode(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Mqtt(MqttSourceEntity::Topic) => {
                IngestSource::Mqtt(MqttSource::Topic(MqttTopicSource {
                    client: self.name(),
                    topic: self.string(),
                    instances: self.positive_u64(),
                    mode: self.mqtt_mode(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Nats(NatsSourceEntity::Subject) => {
                IngestSource::Nats(NatsSource::Subject(NatsSubjectSource {
                    client: self.name(),
                    subject: self.name(),
                    queue_group: self.name(),
                    instances: self.positive_u64(),
                    mode: NatsIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::RabbitMq(RabbitMqSourceEntity::Queue) => {
                IngestSource::RabbitMq(RabbitMqSource::Queue(RabbitMqQueueSource {
                    client: self.name(),
                    queue: self.name(),
                    instances: self.positive_u64(),
                    mode: RabbitMqIngestMode::AckSequential {
                        timeout: self.duration(),
                        retry_policy: self.retry_policy(),
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Redis(RedisSourceEntity::PubSub) => {
                IngestSource::Redis(RedisSource::PubSub(RedisPubSubSource {
                    client: self.name(),
                    channel: self.name(),
                    mode: RedisPubSubIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Prometheus(PrometheusSourceEntity::Query) => {
                IngestSource::Prometheus(PrometheusSource::Query(PrometheusQuerySource {
                    client: self.name(),
                    query: self.string(),
                    every: self.clock_period(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::ZeroMq(ZeroMqSourceEntity::Pull) => {
                IngestSource::ZeroMq(ZeroMqSource::Pull(ZeroMqPullSource {
                    client: self.name(),
                    mode: ZeroMqIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Sqs(SqsSourceEntity::Queue) => {
                IngestSource::Sqs(SqsSource::Queue(SqsQueueSource {
                    client: self.name(),
                    queue: self.name(),
                    instances: self.positive_u64(),
                    mode: SqsIngestMode::AckSequential {
                        timeout: self.duration(),
                        retry_policy: self.retry_policy(),
                    },
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Endpoint(EndpointSourceEntity::Endpoint) => {
                IngestSource::Endpoint(EndpointSource::Endpoint(EndpointEndpointSource {
                    endpoint: self.name(),
                    mode: EndpointIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Websockets(WebsocketsSourceEntity::Connection) => {
                IngestSource::Websockets(WebsocketsSource::Connection(WebsocketsConnectionSource {
                    client: self.name(),
                    mode: WebsocketsIngestMode::NoAckSequential,
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
            IngestSourceKind::Syslog(SyslogSourceEntity::Listener) => {
                IngestSource::Syslog(SyslogSource::Listener(SyslogListenerSource {
                    client: self.name(),
                    quiesce: IngestQuiesceMode::Suspend,
                }))
            }
        };
        let quiesce = self.supported_quiesce(&source);
        source
            .set_quiesce(quiesce)
            .verified("the quiesce mode was chosen among the modes the source supports");
        Some(source)
    }

    /// One of the quiesce modes `source` supports, as its vocabulary states them.
    fn supported_quiesce(&mut self, source: &IngestSource) -> IngestQuiesceMode {
        let candidates = [
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
        ];
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
            _ => KafkaIngestMode::NoAckParallel,
        }
    }

    fn mqtt_mode(&mut self) -> MqttIngestMode {
        let session = self
            .entropy
            .pick([MqttSession::Clean, MqttSession::Persistent]);
        let qos = self
            .entropy
            .pick([MqttQos::AtMostOnce, MqttQos::AtLeastOnce]);
        match self.entropy.byte() % 4 {
            0 => MqttIngestMode::NoAckSequential { session, qos },
            1 => MqttIngestMode::NoAckParallel { session, qos },
            2 => MqttIngestMode::AckSequential {
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
