//! A Kafka broker that one scenario owns: librdkafka's in-process mock cluster.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One single-broker mock cluster, the librdkafka client whose instance hosts it for as
//!   long as the scenario keeps it, the topics it serves, whether it refuses the writes produced
//!   to it or the offsets its consumer groups commit, and an outage of its broker.
//! - **Depends on.** rust-rdkafka through the Kafka connector's testing re-export.
//! - **Must not know.** Nervix, its emitters, or the shared Kafka test broker.
//!
//! The shared Kafka test broker serves every scenario at once, so no scenario may make it refuse
//! writes. Its one replica also defeats a topic's `min.insync.replicas`, which Kafka caps at the
//! replication factor. The mock broker answers the Kafka protocol for one scenario alone. While a
//! scenario asks it to, it refuses every produce request with `NOT_ENOUGH_REPLICAS`: a producer
//! retries that answer, and the broker stores nothing it refused, so nothing it refused appears once
//! it accepts writes again. While a scenario asks it to, it also refuses offset commits with
//! `GROUP_AUTHORIZATION_FAILED`, an answer a consumer does not retry, and a group's committed offset
//! stays where it was. While its broker is down, it closes every connection and accepts none, as a
//! restarting broker does, and it keeps its topics, their records and its groups' committed offsets
//! for when the broker comes back.

use std::{fmt, io, time::Duration};

use nervix_connector_kafka::testing_rdkafka::{
    config::ClientConfig,
    mocking::MockCluster,
    producer::{DefaultProducerContext, Producer, ThreadedProducer},
    types::{RDKafkaApiKey, RDKafkaRespErr},
};
use nervix_primitives::time::{Instant, sleep_until};

/// How many produce requests a refusing broker answers with `NOT_ENOUGH_REPLICAS` at most: far more
/// than a producer, which retries a refused request about once a second, sends during a scenario.
const REFUSED_PRODUCE_REQUESTS: usize = 10_000;

/// How many offset commits a broker refusing every commit answers with `GROUP_AUTHORIZATION_FAILED`
/// at most: far more than an ingestor, which commits again after each retry backoff, sends during a
/// scenario.
const REFUSED_OFFSET_COMMITS: usize = 10_000;

/// The librdkafka setting that makes a client host a mock cluster of that many brokers.
const MOCK_BROKER_COUNT: &str = "test.mock.num.brokers";

/// The id of the mock cluster's one broker: librdkafka numbers a mock cluster's brokers from one.
const BROKER_ID: i32 = 1;

pub(crate) struct KafkaMockBroker {
    /// The client whose librdkafka instance hosts the mock cluster, which lives as long as it.
    host: ThreadedProducer<DefaultProducerContext>,
    /// Where a client reaches the broker.
    bootstrap_servers: String,
    /// When the broker went down, while it is down.
    down_since: Option<Instant>,
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
            down_since: None,
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

    /// Makes the broker refuse every offset commit with `GROUP_AUTHORIZATION_FAILED` until it
    /// accepts commits again.
    pub(crate) fn refuse_offset_commits(&self) -> io::Result<()> {
        self.refuse_next_offset_commits(REFUSED_OFFSET_COMMITS)
    }

    /// Makes the broker refuse the next `commits` offset commits with `GROUP_AUTHORIZATION_FAILED`
    /// and accept the ones after them.
    pub(crate) fn refuse_next_offset_commits(&self, commits: usize) -> io::Result<()> {
        let refusals = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; commits];
        self.cluster()?
            .request_errors(RDKafkaApiKey::OffsetCommit, &refusals);
        Ok(())
    }

    /// Makes the broker store the offsets its consumer groups commit again.
    pub(crate) fn accept_offset_commits(&self) -> io::Result<()> {
        self.cluster()?
            .clear_request_errors(RDKafkaApiKey::OffsetCommit);
        Ok(())
    }

    /// Holds every answer while the scenario checks progress independent of this broker.
    pub(crate) fn hold_answers(&self) -> io::Result<()> {
        self.cluster()?
            .broker_round_trip_time(BROKER_ID, Duration::from_secs(3600))
            .map_err(io::Error::other)
    }

    /// Releases the broker's answers before the scenario drains its sources.
    pub(crate) fn release_answers(&self) -> io::Result<()> {
        self.cluster()?
            .broker_round_trip_time(BROKER_ID, Duration::ZERO)
            .map_err(io::Error::other)
    }

    /// Takes the broker down: it closes every connection and accepts none until it comes back up.
    pub(crate) fn go_down(&mut self) -> io::Result<()> {
        self.cluster()?
            .broker_down(BROKER_ID)
            .map_err(io::Error::other)?;
        self.down_since = Some(Instant::now());
        Ok(())
    }

    /// Brings the broker back up once it has been down for `outage`, measured from when it went
    /// down, so the outage lasts that long however late the scenario asks.
    pub(crate) async fn come_up_after(&mut self, outage: Duration) -> io::Result<()> {
        let down_since = self.down_since.ok_or_else(|| {
            io::Error::other("the mock Kafka broker comes up only once it is down")
        })?;
        let back_at = down_since
            .checked_add(outage)
            .ok_or_else(|| io::Error::other("the outage outlasts the monotonic clock"))?;
        sleep_until(back_at).await;
        self.cluster()?
            .broker_up(BROKER_ID)
            .map_err(io::Error::other)?;
        self.down_since = None;
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
