use super::*;

pub(in crate::runtime) mod endpoint;
pub(in crate::runtime) mod http;
pub(in crate::runtime) mod kafka;
pub(in crate::runtime) mod kinesis;
pub(in crate::runtime) mod mqtt;
pub(in crate::runtime) mod nats;
pub(in crate::runtime) mod prometheus;
pub(in crate::runtime) mod pulsar;
pub(in crate::runtime) mod rabbitmq;
pub(in crate::runtime) mod redis_pubsub;
pub(in crate::runtime) mod sqs;
pub(in crate::runtime) mod websockets;
pub(in crate::runtime) mod zeromq;

use endpoint::EndpointIngestor;
use http::HttpIngestor;
use kafka::KafkaIngestor;
use kinesis::KinesisIngestor;
use mqtt::MqttIngestor;
use nats::NatsIngestor;
use prometheus::PrometheusIngestor;
use pulsar::PulsarIngestor;
use rabbitmq::RabbitMqIngestor;
use redis_pubsub::RedisPubSubIngestor;
use sqs::SqsIngestor;
use websockets::WebsocketsIngestor;
use zeromq::ZeroMqIngestor;

pub(in crate::runtime) struct IngestorStarter;

impl IngestorStarter {
    pub(in crate::runtime) async fn start_scheduled(
        runtime: &Runtime,
        domain: &Domain,
        source_model: Model,
        ingestor: CreateIngestor,
        kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
    ) -> Result<(), RuntimeError> {
        match (&source_model, &ingestor.source) {
            (Model::ClientHttp(client), IngestSource::Http { .. }) => {
                HttpIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientKinesis(client), IngestSource::Kinesis { .. }) => {
                KinesisIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientKafka(client), IngestSource::Kafka { .. }) => {
                KafkaIngestor::start(
                    runtime,
                    domain,
                    client.clone(),
                    ingestor,
                    kafka_offset_state,
                )
                .await
            }
            (Model::ClientPulsar(client), IngestSource::Pulsar { .. }) => {
                PulsarIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientPrometheus(client), IngestSource::Prometheus { .. }) => {
                PrometheusIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientRabbitMq(client), IngestSource::RabbitMq { .. }) => {
                RabbitMqIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientRedis(client), IngestSource::RedisPubSub { .. }) => {
                RedisPubSubIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientMqtt(client), IngestSource::Mqtt { .. }) => {
                MqttIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientNats(client), IngestSource::Nats { .. }) => {
                NatsIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientZeroMq(client), IngestSource::ZeroMq { .. }) => {
                ZeroMqIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientSqs(client), IngestSource::Sqs { .. }) => {
                SqsIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::ClientWebsockets(client), IngestSource::Websockets { .. }) => {
                WebsocketsIngestor::start(runtime, domain, client.clone(), ingestor).await
            }
            (Model::Endpoint(endpoint), IngestSource::Endpoint { .. }) => {
                EndpointIngestor::start(runtime, domain, endpoint.clone(), ingestor).await
            }
            _ => Err(RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: "source kind does not match ingestor source".to_string(),
            }),
        }
    }
}
