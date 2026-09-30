//! Ingestors: the transport or client source they read, and the routes that build their records.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, ClientIngestMode, ClientIngestSource, CreateIngestor, EndpointIngestMode,
    IngestQuiesceMode, IngestQuiesceOverflow, IngestSource, IngestTimestampSource, IngestorInput,
    KafkaIngestMode, KafkaOffsetMode, MqttIngestMode, MqttQos, MqttSession, NatsIngestMode,
    RabbitMqIngestMode, RedisPubSubIngestMode, RetryPolicy, SqsIngestMode, TransportIngestorInput,
    WebsocketsIngestMode, ZeroMqIngestMode,
};

use crate::{
    Arbitrary,
    route::{RouteBranch, RouteFlush, RouteShape},
};

impl Arbitrary<'_> {
    /// An ingestor over a transport or a client source, whose every route constructs its own
    /// outgoing branch.
    pub fn create_ingestor(&mut self) -> CreateIngestor {
        let input = if self.entropy.byte().is_multiple_of(8) {
            IngestorInput::Client(ClientIngestSource {
                schema: self.name(),
                mode: ClientIngestMode {
                    window: self.ack_window(),
                    ack_timeout: self.duration(),
                    retry_policy: self.retry_policy(),
                },
            })
        } else {
            IngestorInput::Transport(TransportIngestorInput {
                source: self.ingest_source(),
                codec: self.name(),
            })
        };
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
            filter_where: self.filter_expression(),
        }
    }

    /// A transport source of any kind, quiescing in a mode that kind supports.
    pub fn ingest_source(&mut self) -> IngestSource {
        let mut source = match self.entropy.byte() % 13 {
            0 => IngestSource::Http {
                client: self.name(),
                every: self.clock_period(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            1 => IngestSource::Kafka {
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
            },
            2 => IngestSource::Pulsar {
                client: self.name(),
                topic: self.name(),
                subscription: self.name(),
                instances: self.positive_u64(),
                mode: self.kafka_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            3 => IngestSource::Mqtt {
                client: self.name(),
                topic: self.string(),
                instances: self.positive_u64(),
                mode: self.mqtt_mode(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            4 => IngestSource::Nats {
                client: self.name(),
                subject: self.name(),
                queue_group: self.name(),
                instances: self.positive_u64(),
                mode: NatsIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            5 => IngestSource::RabbitMq {
                client: self.name(),
                queue: self.name(),
                instances: self.positive_u64(),
                mode: RabbitMqIngestMode::AckSequential {
                    timeout: self.duration(),
                    retry_policy: self.retry_policy(),
                },
                quiesce: IngestQuiesceMode::Suspend,
            },
            6 => IngestSource::RedisPubSub {
                client: self.name(),
                channel: self.name(),
                mode: RedisPubSubIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            7 => IngestSource::Prometheus {
                client: self.name(),
                query: self.string(),
                every: self.clock_period(),
                quiesce: IngestQuiesceMode::Suspend,
            },
            8 => IngestSource::ZeroMq {
                client: self.name(),
                mode: ZeroMqIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            9 => IngestSource::Sqs {
                client: self.name(),
                queue: self.name(),
                instances: self.positive_u64(),
                mode: SqsIngestMode::AckSequential {
                    timeout: self.duration(),
                    retry_policy: self.retry_policy(),
                },
                quiesce: IngestQuiesceMode::Suspend,
            },
            10 => IngestSource::Endpoint {
                endpoint: self.name(),
                mode: EndpointIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            11 => IngestSource::Websockets {
                client: self.name(),
                mode: WebsocketsIngestMode::NoAckSequential,
                quiesce: IngestQuiesceMode::Suspend,
            },
            _ => IngestSource::Syslog {
                client: self.name(),
                quiesce: IngestQuiesceMode::Suspend,
            },
        };
        let quiesce = self.supported_quiesce(&source);
        source
            .set_quiesce(quiesce)
            .verified("the quiesce mode was chosen among the modes the source supports");
        source
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
