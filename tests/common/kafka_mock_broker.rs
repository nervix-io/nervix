//! A Kafka broker that one scenario owns: librdkafka's in-process mock cluster.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One single-broker mock cluster, the librdkafka client whose instance hosts it for as
//!   long as the scenario keeps it, the topics it serves, and whether it refuses the writes produced
//!   to it.
//! - **Depends on.** rust-rdkafka through the Kafka connector's testing re-export.
//! - **Must not know.** Nervix, its emitters, or the shared Kafka test broker.
//!
//! The shared Kafka test broker serves every scenario at once, so no scenario may make it refuse
//! writes. Its one replica also defeats a topic's `min.insync.replicas`, which Kafka caps at the
//! replication factor. The mock broker answers the Kafka protocol for one scenario alone. While a
//! scenario asks it to, it refuses every produce request with `NOT_ENOUGH_REPLICAS`: a producer
//! retries that answer, and the broker stores nothing it refused, so nothing it refused appears once
//! it accepts writes again.

use std::{fmt, io};

use nervix_connector_kafka::testing_rdkafka::{
    config::ClientConfig,
    mocking::MockCluster,
    producer::{DefaultProducerContext, Producer, ThreadedProducer},
    types::{RDKafkaApiKey, RDKafkaRespErr},
};

/// How many produce requests a refusing broker answers with `NOT_ENOUGH_REPLICAS` at most: far more
/// than a producer, which retries a refused request about once a second, sends during a scenario.
const REFUSED_PRODUCE_REQUESTS: usize = 10_000;

/// The librdkafka setting that makes a client host a mock cluster of that many brokers.
const MOCK_BROKER_COUNT: &str = "test.mock.num.brokers";

pub(crate) struct KafkaMockBroker {
    /// The client whose librdkafka instance hosts the mock cluster, which lives as long as it.
    host: ThreadedProducer<DefaultProducerContext>,
    /// Where a client reaches the broker.
    bootstrap_servers: String,
}

impl KafkaMockBroker {
    /// Starts a mock cluster of one broker.
    pub(crate) fn start() -> io::Result<Self> {
        let host: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set(MOCK_BROKER_COUNT, "1")
            .create()
            .map_err(io::Error::other)?;
        let bootstrap_servers = Self::cluster_of(&host)?.bootstrap_servers();
        Ok(Self {
            host,
            bootstrap_servers,
        })
    }

    fn cluster_of(
        host: &ThreadedProducer<DefaultProducerContext>,
    ) -> io::Result<MockCluster<'_, DefaultProducerContext>> {
        host.client().mock_cluster().ok_or_else(|| {
            io::Error::other(format!(
                "a client created with {MOCK_BROKER_COUNT} hosts no mock cluster"
            ))
        })
    }

    fn cluster(&self) -> io::Result<MockCluster<'_, DefaultProducerContext>> {
        Self::cluster_of(&self.host)
    }

    /// Where a client reaches the broker, as `bootstrap.servers` names it.
    pub(crate) fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// A client configuration that reaches the broker.
    pub(crate) fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config
    }

    /// Creates `topic` with one partition.
    pub(crate) fn create_topic(&self, topic: &str) -> io::Result<()> {
        self.cluster()?
            .create_topic(topic, 1, 1)
            .map_err(io::Error::other)
    }

    /// Makes the broker refuse every produce request with `NOT_ENOUGH_REPLICAS` until it accepts
    /// writes again.
    pub(crate) fn refuse_writes(&self) -> io::Result<()> {
        let refusals =
            vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS; REFUSED_PRODUCE_REQUESTS];
        self.cluster()?
            .request_errors(RDKafkaApiKey::Produce, &refusals);
        Ok(())
    }

    /// Makes the broker store the writes produced to it again.
    pub(crate) fn accept_writes(&self) -> io::Result<()> {
        self.cluster()?.clear_request_errors(RDKafkaApiKey::Produce);
        Ok(())
    }
}

impl fmt::Debug for KafkaMockBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaMockBroker")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .finish_non_exhaustive()
    }
}
