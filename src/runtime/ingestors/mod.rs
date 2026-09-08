use super::*;

pub(in crate::runtime) mod endpoint;
pub(in crate::runtime) mod http;
pub(in crate::runtime) mod kafka;
pub(in crate::runtime) mod mqtt;
pub(in crate::runtime) mod nats;
pub(in crate::runtime) mod prometheus;
pub(in crate::runtime) mod pulsar;
pub(in crate::runtime) mod rabbitmq;
pub(in crate::runtime) mod redis_pubsub;
pub(in crate::runtime) mod sqs;
pub(in crate::runtime) mod syslog;
pub(in crate::runtime) mod websockets;
pub(in crate::runtime) mod zeromq;

use endpoint::EndpointIngestor;
use http::HttpIngestor;
use kafka::KafkaIngestor;
use mqtt::MqttIngestor;
use nats::NatsIngestor;
use prometheus::PrometheusIngestor;
use pulsar::PulsarIngestor;
use rabbitmq::RabbitMqIngestor;
use redis_pubsub::RedisPubSubIngestor;
use sqs::SqsIngestor;
use syslog::SyslogIngestor;
use websockets::WebsocketsIngestor;
use zeromq::ZeroMqIngestor;

pub(in crate::runtime) struct IngestorStarter;

impl IngestorStarter {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: IngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        runtime.prepare_ingestor_quiescence(&plan.ingestor().domain, plan.ingestor());
        match plan {
            IngestorStartPlan::Http(plan) => HttpIngestor::start(runtime, plan).await,
            IngestorStartPlan::Kafka(plan) => {
                let local_node_id = runtime.inner.remote_dispatch.local_node_id.read().clone();
                let kafka_offset_state = plan.offset_state_placement.as_ref().and_then(|planned| {
                    local_node_id
                        .as_ref()
                        .is_some_and(|local| Some(local) == planned.primary_node.as_ref())
                        .then(|| {
                            runtime
                                .inner
                                .replicated_kafka_offset_states
                                .get(&planned.placement)
                                .map(|state| state.value().clone())
                        })
                        .flatten()
                });
                KafkaIngestor::start(runtime, plan, kafka_offset_state).await
            }
            IngestorStartPlan::Pulsar(plan) => PulsarIngestor::start(runtime, plan).await,
            IngestorStartPlan::Prometheus(plan) => PrometheusIngestor::start(runtime, plan).await,
            IngestorStartPlan::RabbitMq(plan) => RabbitMqIngestor::start(runtime, plan).await,
            IngestorStartPlan::RedisPubSub(plan) => RedisPubSubIngestor::start(runtime, plan).await,
            IngestorStartPlan::Mqtt(plan) => MqttIngestor::start(runtime, plan).await,
            IngestorStartPlan::Nats(plan) => NatsIngestor::start(runtime, plan).await,
            IngestorStartPlan::ZeroMq(plan) => ZeroMqIngestor::start(runtime, plan).await,
            IngestorStartPlan::Sqs(plan) => SqsIngestor::start(runtime, plan).await,
            IngestorStartPlan::Websockets(plan) => WebsocketsIngestor::start(runtime, plan).await,
            IngestorStartPlan::Syslog(plan) => SyslogIngestor::start(runtime, plan).await,
            IngestorStartPlan::Endpoint(plan) => EndpointIngestor::start(runtime, plan).await,
        }
    }
}
