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
mod source;
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
                let dispatcher = runtime.inner.remote_dispatcher.load_full();
                let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
                let kafka_offset_state = plan.offset_state_placement.as_ref().and_then(|planned| {
                    local_node_id
                        .is_some_and(|local| Some(local) == planned.primary_node.as_ref())
                        .then(|| {
                            runtime
                                .inner
                                .replicated_kafka_offset_states
                                .get(&planned.placement)
                                .and_then(|state| {
                                    ReplicatedKafkaOffsetState::current_originator(state.value())
                                })
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

#[cfg(test)]
mod tests {
    use nervix_models::{
        ClientConfigEntry, CreateClientHttp, CreateClientPrometheus, CreateClientWebsockets,
        CreateClientZeroMq,
    };

    use super::*;

    #[test]
    fn client_config_extractors_handle_defaults_and_missing_keys() {
        let zeromq = CreateClientZeroMq::<u64> {
            name: named("zmq"),
            mount: None,
            config: vec![
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: "tcp://127.0.0.1:5555".to_string(),
                },
                ClientConfigEntry {
                    key: "bind".to_string(),
                    value: "TRUE".to_string(),
                },
            ],
        };
        assert_eq!(
            ingestors::zeromq::ZeroMqIngestor::addr_from_config(&zeromq.config).expect("addr"),
            "tcp://127.0.0.1:5555"
        );
        assert!(ingestors::zeromq::ZeroMqIngestor::bind_from_config(
            &zeromq.config
        ));

        let http = CreateClientHttp::<u64> {
            name: named("http"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "https://example.com/api".to_string(),
            }],
        };
        assert_eq!(
            ingestors::http::HttpIngestor::endpoint_from_config(&http.config).expect("endpoint"),
            "https://example.com/api"
        );
        assert_eq!(
            ingestors::http::HttpIngestor::method_from_config(&http.config)
                .expect("default method"),
            reqwest::Method::GET
        );

        let http_post = CreateClientHttp::<u64> {
            name: named("http"),
            mount: None,
            config: vec![
                ClientConfigEntry {
                    key: "endpoint".to_string(),
                    value: "https://example.com/api".to_string(),
                },
                ClientConfigEntry {
                    key: "method".to_string(),
                    value: "POST".to_string(),
                },
            ],
        };
        assert_eq!(
            ingestors::http::HttpIngestor::method_from_config(&http_post.config)
                .expect("post method"),
            reqwest::Method::POST
        );
        assert!(
            ingestors::http::HttpIngestor::method_from_config(
                &CreateClientHttp::<u64> {
                    name: named("http"),
                    mount: None,
                    config: vec![ClientConfigEntry {
                        key: "method".to_string(),
                        value: "NOT A METHOD".to_string(),
                    }],
                }
                .config
            )
            .is_err()
        );

        let websocket = CreateClientWebsockets::<u64> {
            name: named("ws"),
            mount: None,
            signaling_protocol: None,
            config: vec![ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "wss://example.com/socket".to_string(),
            }],
        };
        assert_eq!(
            ingestors::websockets::WebsocketsIngestor::endpoint_from_config(&websocket.config)
                .expect("endpoint"),
            "wss://example.com/socket"
        );

        let prometheus = CreateClientPrometheus::<u64> {
            name: named("prom"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: "http://prometheus:9090".to_string(),
            }],
        };
        assert_eq!(
            ingestors::prometheus::PrometheusIngestor::addr_from_config(&prometheus.config)
                .expect("addr"),
            "http://prometheus:9090"
        );

        let zeromq_default = CreateClientZeroMq::<u64> {
            name: named("zmq"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: "tcp://127.0.0.1:5555".to_string(),
            }],
        };
        assert!(!ingestors::zeromq::ZeroMqIngestor::bind_from_config(
            &zeromq_default.config
        ));

        assert!(
            ingestors::zeromq::ZeroMqIngestor::addr_from_config(
                &CreateClientZeroMq::<u64> {
                    name: named("zmq"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing zeromq addr")
            .to_string()
            .contains("missing ZeroMQ client config key 'addr'")
        );
        assert!(
            ingestors::http::HttpIngestor::endpoint_from_config(
                &CreateClientHttp::<u64> {
                    name: named("http"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing http endpoint")
            .to_string()
            .contains("missing HTTP client config key 'endpoint'")
        );
        assert!(
            ingestors::websockets::WebsocketsIngestor::endpoint_from_config(
                &CreateClientWebsockets::<u64> {
                    name: named("ws"),
                    mount: None,
                    signaling_protocol: None,
                    config: vec![],
                }
                .config,
            )
            .expect_err("missing websocket endpoint")
            .to_string()
            .contains("missing WebSockets client config key 'endpoint'")
        );
        assert!(
            ingestors::prometheus::PrometheusIngestor::addr_from_config(
                &CreateClientPrometheus::<u64> {
                    name: named("prom"),
                    mount: None,
                    config: vec![],
                }
                .config
            )
            .expect_err("missing prometheus addr")
            .to_string()
            .contains("missing Prometheus client config key 'addr'")
        );
    }
}
