//! NATS JetStream ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The durable pull consumer each NATS STREAM ingestor instance reads, the check that
//!   an operator provisioned it as a pull consumer that acknowledges each message explicitly,
//!   message headers as ingest headers, and the acknowledgement and negative acknowledgement of
//!   each delivery.
//! - **Depends on.** The connector contract, typed client configuration entries, the crate's NATS
//!   client connection, `error-stack` and `async-nats`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::fmt;

use async_nats::{
    Message as NatsMessage, Subject,
    jetstream::{
        consumer::{AckPolicy, Config as ConsumerConfig, FromConsumer as _, PullConsumer, pull},
        context::{ConsumerInfoError, ConsumerInfoErrorKind},
    },
};
use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorBatch,
    IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, IngestorSettlement,
};
use nervix_models::{ClientConfigEntry, ConnectorKind, NatsConsumerName, NatsStreamName};
use nervix_primitives::sync::mpsc;
use thiserror::Error;

use super::pull::{NatsPullLimits, NatsStreamPull};
use crate::subject::{NatsMessageHeaders, NatsSubjectIngestorPlan, PENDING_LOSS_SIGNALS};

const NATS: &str = "nats";

/// What a NATS server answered when it refused a pull request, or ended it in a way a new one on
/// the same connection does not repair: its status code, and the description it gave, absent when
/// it gave none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsPullRefusal {
    pub status: u16,
    pub description: Option<String>,
}

impl fmt::Display for NatsPullRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "status {}", self.status)?;
        if let Some(description) = &self.description {
            write!(formatter, ": {description}")?;
        }
        Ok(())
    }
}

/// Why a NATS STREAM ingestor could not bind its consumer, pull from it, or settle a delivery.
#[derive(Debug, Error)]
pub enum NatsStreamIngestorError {
    /// The client could not connect; the NATS client failure beneath it says why.
    #[error("failed to connect the NATS client of a stream ingestor")]
    Connect,
    /// The stream an operator names does not exist. Nervix never creates one, so the ingestor
    /// reports it until the operator provisions it.
    #[error("{connector} stream '{stream}' does not exist", connector = ConnectorKind::Nats)]
    MissingStream { stream: NatsStreamName },
    /// The consumer an operator names does not exist on its stream. Nervix never creates one, so
    /// the ingestor reports it until the operator provisions it.
    #[error(
        "{connector} consumer '{consumer}' of stream '{stream}' does not exist",
        connector = ConnectorKind::Nats
    )]
    MissingConsumer {
        stream: NatsStreamName,
        consumer: NatsConsumerName,
    },
    /// Whether the consumer exists could not be learned; the client's error is the cause beneath.
    #[error(
        "failed to verify {connector} consumer '{consumer}' of stream '{stream}'",
        connector = ConnectorKind::Nats
    )]
    VerifyConsumer {
        stream: NatsStreamName,
        consumer: NatsConsumerName,
    },
    /// The consumer delivers to a subject of its own, so no pull request reaches it.
    #[error(
        "{connector} consumer '{consumer}' of stream '{stream}' is a push consumer; a stream \
         ingestor reads a pull consumer",
        connector = ConnectorKind::Nats
    )]
    PushConsumer {
        stream: NatsStreamName,
        consumer: NatsConsumerName,
    },
    /// The consumer does not wait for an acknowledgement of each message, so the ingestor could
    /// not hold one back until its records are delivered.
    #[error(
        "{connector} consumer '{consumer}' of stream '{stream}' acknowledges with policy {policy:?}; \
         a stream ingestor acknowledges each message explicitly",
        connector = ConnectorKind::Nats
    )]
    AcknowledgementPolicy {
        stream: NatsStreamName,
        consumer: NatsConsumerName,
        policy: AckPolicy,
    },
    #[error(
        "failed to subscribe to the inbox of {connector} consumer '{consumer}' of stream \
         '{stream}'",
        connector = ConnectorKind::Nats
    )]
    Subscribe {
        stream: NatsStreamName,
        consumer: NatsConsumerName,
    },
    #[error("failed to request deliveries from a NATS JetStream consumer")]
    Pull,
    /// The server refused a pull request or ended it in a way a new one on the same connection does
    /// not repair.
    #[error("the NATS JetStream consumer refused a pull request with {refusal}")]
    PullRefused { refusal: NatsPullRefusal },
    #[error("a NATS JetStream delivery arrived with no pull request outstanding")]
    UnrequestedDelivery,
    #[error("a NATS JetStream delivery carries no acknowledgement subject")]
    UnacknowledgeableDelivery,
    /// The client lost its connection to the server, which ends the pull that connection carried.
    #[error("NATS connection lost")]
    ConnectionLost,
    /// The instance holds no connection to settle a delivery on: it suspended, or a read failure
    /// ended its pull.
    #[error("no NATS connection to settle a JetStream delivery on")]
    Disconnected,
    #[error("failed to acknowledge a NATS JetStream delivery")]
    Acknowledge,
    #[error("failed to negatively acknowledge a NATS JetStream delivery")]
    NegativelyAcknowledge,
    #[error("NATS batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

pub(super) type NatsStreamIngestorResult<T> = Result<T, Report<NatsStreamIngestorError>>;

/// What one NATS STREAM ingestor reads: its resolved client entries, the stream, and the durable
/// pull consumer of that stream an operator provisioned.
#[derive(Clone)]
pub struct NatsStreamIngestorPlan {
    config: Vec<ClientConfigEntry>,
    stream: NatsStreamName,
    consumer: NatsConsumerName,
}

impl NatsStreamIngestorPlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        stream: NatsStreamName,
        consumer: NatsConsumerName,
    ) -> Self {
        Self {
            config,
            stream,
            consumer,
        }
    }

    /// Connects, verifies the consumer and opens the inbox its pull requests deliver to.
    ///
    /// The consumer is looked up, never created: a missing stream or consumer is reported by name
    /// until an operator provisions it.
    async fn pull(&self) -> NatsStreamIngestorResult<NatsStreamPull> {
        let (lost, connection_lost) = mpsc::channel(PENDING_LOSS_SIGNALS);
        let client = NatsSubjectIngestorPlan::client_from_config(&self.config, lost)
            .await
            .change_context(NatsStreamIngestorError::Connect)?;
        let context = async_nats::jetstream::new(client.clone());
        let stream = context
            .get_stream_no_info(self.stream.as_str())
            .await
            .map_err(|source| Report::new(source).change_context(self.verify_failure()))?;
        let info = stream
            .consumer_info(self.consumer.as_str())
            .await
            .map_err(|error| self.lookup_failure(error))?;
        self.check(&info.config)?;
        let limits = NatsPullLimits::of(&info.config);
        let config = pull::Config::try_from_consumer_config(info.config.clone()).verified(
            "the check above refused a consumer with a delivery subject, the one configuration a \
             pull consumer cannot hold",
        );
        let consumer = PullConsumer::new(config, info, context);
        let inbox = Subject::from(client.new_inbox());
        let deliveries = client.subscribe(inbox.clone()).await.map_err(|source| {
            Report::new(source).change_context(NatsStreamIngestorError::Subscribe {
                stream: self.stream.clone(),
                consumer: self.consumer.clone(),
            })
        })?;
        Ok(NatsStreamPull::new(
            client,
            consumer,
            deliveries,
            inbox,
            connection_lost,
            limits,
        ))
    }

    /// What a failed consumer lookup means: the server's answer that the stream or the consumer
    /// does not exist, or a lookup that learned nothing.
    fn lookup_failure(&self, error: ConsumerInfoError) -> Report<NatsStreamIngestorError> {
        match error.kind() {
            ConsumerInfoErrorKind::StreamNotFound => {
                Report::new(NatsStreamIngestorError::MissingStream {
                    stream: self.stream.clone(),
                })
            }
            ConsumerInfoErrorKind::NotFound => {
                Report::new(NatsStreamIngestorError::MissingConsumer {
                    stream: self.stream.clone(),
                    consumer: self.consumer.clone(),
                })
            }
            _ => Report::new(error).change_context(self.verify_failure()),
        }
    }

    fn verify_failure(&self) -> NatsStreamIngestorError {
        NatsStreamIngestorError::VerifyConsumer {
            stream: self.stream.clone(),
            consumer: self.consumer.clone(),
        }
    }

    /// Refuses a consumer the ingestor cannot read as it declares: one that pushes its deliveries,
    /// or one that does not wait for each message's own acknowledgement.
    fn check(&self, config: &ConsumerConfig) -> NatsStreamIngestorResult<()> {
        if config.deliver_subject.is_some() {
            return Err(Report::new(NatsStreamIngestorError::PushConsumer {
                stream: self.stream.clone(),
                consumer: self.consumer.clone(),
            }));
        }
        if config.ack_policy != AckPolicy::Explicit {
            return Err(Report::new(
                NatsStreamIngestorError::AcknowledgementPolicy {
                    stream: self.stream.clone(),
                    consumer: self.consumer.clone(),
                    policy: config.ack_policy,
                },
            ));
        }
        Ok(())
    }
}

/// The subject one delivery is acknowledged on.
///
/// JetStream addresses an acknowledgement to the delivery, not to the connection that received it,
/// but the consumer delivers again everything a lost connection held unacknowledged once its
/// `AckWait` passes, so the host treats a position as its connection's.
#[derive(Debug, Clone)]
pub struct NatsStreamIngestorPosition {
    acknowledgement: Subject,
}

impl NatsStreamIngestorPosition {
    pub(super) fn acknowledgement_subject(&self) -> Subject {
        self.acknowledgement.clone()
    }
}

/// One delivery of the consumer, whose headers are its ingest headers.
pub struct NatsStreamIngestorMessage {
    headers: NatsMessageHeaders,
    position: NatsStreamIngestorPosition,
}

impl NatsStreamIngestorMessage {
    /// The delivery `message` carries, with the subject it is acknowledged on.
    pub(super) fn delivered(message: NatsMessage) -> NatsStreamIngestorResult<Self> {
        let Some(acknowledgement) = message.reply.clone() else {
            return Err(Report::new(
                NatsStreamIngestorError::UnacknowledgeableDelivery,
            ));
        };
        Ok(Self {
            headers: NatsMessageHeaders::new(message),
            position: NatsStreamIngestorPosition { acknowledgement },
        })
    }
}

impl IngestorMessage for NatsStreamIngestorMessage {
    type Position = NatsStreamIngestorPosition;

    fn payload(&self) -> &[u8] {
        self.headers.payload()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::headers(&self.headers)
    }
}

/// One NATS STREAM ingestor instance: its pull, absent while suspended or reconnecting.
pub struct NatsStreamIngestor {
    plan: NatsStreamIngestorPlan,
    pull: Option<NatsStreamPull>,
}

impl NatsStreamIngestor {
    /// The pull a settlement goes through, or the failure of an instance that holds none.
    fn settling(&self, failure: IngestorError) -> IngestorResult<&NatsStreamPull> {
        let Some(pull) = self.pull.as_ref() else {
            return Err(Report::new(NatsStreamIngestorError::Disconnected).change_context(failure));
        };
        Ok(pull)
    }

    /// Lets the pull go, returning to the consumer what the client received for it.
    async fn release(&mut self) {
        let Some(pull) = self.pull.take() else {
            return;
        };
        pull.release().await;
    }
}

#[async_trait]
impl IngestorConnector for NatsStreamIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Nats(nervix_models::NatsIngestorEntity::Stream);

    type Plan = NatsStreamIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            pull: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.pull.is_none()
    }

    /// Suspension lets the pull go: its inbox unsubscribes, so the consumer delivers nothing to the
    /// instance and the stream keeps every message the consumer has not delivered.
    async fn suspend(&mut self) -> IngestorResult<()> {
        self.release().await;
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.pull.is_none() {
            let pull = self
                .plan
                .pull()
                .await
                .change_context(IngestorError::Resume { connector: NATS })?;
            self.pull = Some(pull);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.release().await;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for NatsStreamIngestor {
    type Message = NatsStreamIngestorMessage;
    type Position = NatsStreamIngestorPosition;

    /// Every delivery is acknowledged or negatively acknowledged on its own subject, independent
    /// of the deliveries beside it.
    const SETTLEMENT: IngestorSettlement = IngestorSettlement::Individual;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(pull) = self.pull.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        let batch = pull.read(request, cancellation).await;
        if batch.is_err() {
            self.pull = None;
        }
        batch.change_context(IngestorError::Read { connector: NATS })
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let failure = IngestorError::Acknowledge { connector: NATS };
        let pull = self.settling(failure)?;
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            pull.acknowledge(position).await.change_context(failure)?;
        }
        Ok(())
    }

    /// A rejected delivery is negatively acknowledged, so the consumer delivers it again on the
    /// host's next read rather than once its `AckWait` passes.
    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let failure = IngestorError::Reject { connector: NATS };
        let pull = self.settling(failure)?;
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            pull.reject(position).await.change_context(failure)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use futures_util::future::join;
    use meticulous::OptionExt as _;
    use nervix_primitives::{sync::oneshot, time::timeout};
    use serde_json::json;

    use super::*;
    use crate::subject::scripted_server::{ScriptedConnection, ScriptedServer, Subscription};

    /// How long a check waits for the ingestor or the stand-in server to observe what the other
    /// did. It bounds a wait for an event that follows at once on a loopback connection, so only
    /// an ingestor that never observes it reaches the bound.
    const OBSERVATION_BOUND: Duration = Duration::from_secs(30);
    /// The JetStream API subject a pull of consumer `nervix` of stream `orders` asks on.
    const PULL_SUBJECT: &str = "$JS.API.CONSUMER.MSG.NEXT.orders.nervix";
    const SEQUENTIAL: IngestorBatchRequest = IngestorBatchRequest {
        max_messages: NonZeroUsize::MIN,
        batch_timeout: None,
    };

    fn plan(addr: &str) -> NatsStreamIngestorPlan {
        NatsStreamIngestorPlan::new(
            vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
            NatsStreamName::try_from("orders").assured("a fixed stream name is valid"),
            NatsConsumerName::try_from("nervix").assured("a fixed consumer name is valid"),
        )
    }

    /// The configuration a server reports for durable pull consumer `nervix` that acknowledges each
    /// message explicitly, as nats-server 2.11 answers its lookup.
    fn pull_consumer() -> serde_json::Value {
        json!({
            "durable_name": "nervix",
            "name": "nervix",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "ack_wait": 30_000_000_000_u64,
            "max_deliver": -1,
            "replay_policy": "instant",
            "max_waiting": 512,
            "max_ack_pending": 1000,
            "num_replicas": 0,
        })
    }

    /// A server's answer to the lookup of consumer `nervix` of stream `orders` configured as
    /// `config`.
    fn consumer_info(config: serde_json::Value) -> String {
        json!({
            "type": "io.nats.jetstream.api.v1.consumer_info_response",
            "stream_name": "orders",
            "name": "nervix",
            "created": "2026-10-29T06:57:46.883687662Z",
            "config": config,
            "delivered": {"consumer_seq": 0, "stream_seq": 0},
            "ack_floor": {"consumer_seq": 0, "stream_seq": 0},
            "num_ack_pending": 0,
            "num_redelivered": 0,
            "num_waiting": 0,
            "num_pending": 0,
            "ts": "2026-10-29T06:57:46.884728592Z",
        })
        .to_string()
    }

    /// A server's refusal of the lookup with JetStream error `err_code`.
    fn lookup_refusal(err_code: u64, description: &str) -> String {
        json!({
            "type": "io.nats.jetstream.api.v1.consumer_info_response",
            "error": {"code": 404, "err_code": err_code, "description": description},
        })
        .to_string()
    }

    /// Greets the instance's connection, answers its consumer lookup with `answer` and returns the
    /// connection.
    async fn answer_lookup(server: &ScriptedServer, answer: &str) -> ScriptedConnection {
        let mut connection = server.greet(1024 * 1024).await;
        let lookup = connection.expect_publish().await;
        assert_eq!(lookup.subject, "$JS.API.CONSUMER.INFO.orders.nervix");
        connection.reply(&lookup, answer).await;
        connection
    }

    /// Greets the instance, answers its lookup for a pull consumer and returns the connection with
    /// the inbox its pull requests deliver to.
    async fn bind(server: &ScriptedServer) -> (ScriptedConnection, Subscription) {
        let mut connection = answer_lookup(server, &consumer_info(pull_consumer())).await;
        let inbox = connection.expect_subscription().await;
        (connection, inbox)
    }

    /// Reads the next pull request and checks that it asks consumer `nervix` for `batch` deliveries
    /// without expiry.
    async fn expect_pull(connection: &mut ScriptedConnection, batch: u64) {
        let pull = connection.expect_publish().await;
        assert_eq!(pull.subject, PULL_SUBJECT);
        let request: serde_json::Value =
            serde_json::from_slice(&pull.message).assured("a pull request is a JSON object");
        assert_eq!(request["batch"], json!(batch), "{request}");
        assert!(
            request.get("expires").is_none(),
            "a pull request waits without expiry: {request}"
        );
    }

    /// Answers the next settlement the instance sends, which must be `kind` on `subject`.
    async fn answer_settlement(connection: &mut ScriptedConnection, subject: &str, kind: &[u8]) {
        let settlement = connection.expect_publish().await;
        assert_eq!(settlement.subject, subject);
        assert_eq!(settlement.message, kind);
        connection.reply(&settlement, "").await;
    }

    async fn opened(plan: &NatsStreamIngestorPlan) -> NatsStreamIngestor {
        let mut source = NatsStreamIngestor::open(plan, 0)
            .await
            .assured("opening a NATS STREAM ingestor connects nothing");
        timeout(OBSERVATION_BOUND, source.resume())
            .await
            .assured("the stand-in server answers the lookup within the bound")
            .assured("the instance binds the pull consumer");
        source
    }

    async fn read(
        source: &mut NatsStreamIngestor,
        request: IngestorBatchRequest,
    ) -> Vec<NatsStreamIngestorMessage> {
        let batch = timeout(
            OBSERVATION_BOUND,
            source.next_batch(request, &IngestorReadCancellation::new()),
        )
        .await
        .assured("the stand-in server delivers within the bound")
        .assured("the read takes what the pull brought");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("an uncancelled read ends with the deliveries it took");
        };
        messages
    }

    fn payloads(messages: &[NatsStreamIngestorMessage]) -> Vec<Vec<u8>> {
        messages
            .iter()
            .map(|message| message.payload().to_vec())
            .collect()
    }

    #[nervix_primitives::test]
    async fn a_missing_stream_or_consumer_is_reported_by_name() {
        for (answer, expected) in [
            (
                lookup_refusal(10059, "stream not found"),
                "NATS stream 'orders' does not exist",
            ),
            (
                lookup_refusal(10014, "consumer not found"),
                "NATS consumer 'nervix' of stream 'orders' does not exist",
            ),
        ] {
            let server = ScriptedServer::start().await;
            let plan = plan(&server.addr());
            let script = nervix_primitives::task::spawn(async move {
                answer_lookup(&server, &answer).await;
            });
            let mut source = NatsStreamIngestor::open(&plan, 0)
                .await
                .assured("opening a NATS STREAM ingestor connects nothing");
            let error = timeout(OBSERVATION_BOUND, source.resume())
                .await
                .assured("the stand-in server answers the lookup within the bound")
                .expect_err("a consumer that does not exist fails the resume");
            assert!(matches!(
                error.current_context(),
                IngestorError::Resume { connector: NATS }
            ));
            let cause = error
                .downcast_ref::<NatsStreamIngestorError>()
                .assured("the typed cause stays beneath the resume failure");
            assert_eq!(cause.to_string(), expected);
            assert!(source.needs_resume());
            script.await.assured("the script answers the lookup");
        }
    }

    #[nervix_primitives::test]
    async fn a_consumer_the_ingestor_cannot_read_as_it_declares_is_refused() {
        let mut push = pull_consumer();
        push["deliver_subject"] = json!("deliveries");
        let mut acknowledges_all = pull_consumer();
        acknowledges_all["ack_policy"] = json!("all");
        let mut acknowledges_none = pull_consumer();
        acknowledges_none["ack_policy"] = json!("none");
        for (config, expected) in [
            (
                push,
                "NATS consumer 'nervix' of stream 'orders' is a push consumer; a stream ingestor \
                 reads a pull consumer",
            ),
            (
                acknowledges_all,
                "NATS consumer 'nervix' of stream 'orders' acknowledges with policy All; a stream \
                 ingestor acknowledges each message explicitly",
            ),
            (
                acknowledges_none,
                "NATS consumer 'nervix' of stream 'orders' acknowledges with policy None; a \
                 stream ingestor acknowledges each message explicitly",
            ),
        ] {
            let server = ScriptedServer::start().await;
            let plan = plan(&server.addr());
            let script = nervix_primitives::task::spawn(async move {
                answer_lookup(&server, &consumer_info(config)).await;
            });
            let mut source = NatsStreamIngestor::open(&plan, 0)
                .await
                .assured("opening a NATS STREAM ingestor connects nothing");
            let error = timeout(OBSERVATION_BOUND, source.resume())
                .await
                .assured("the stand-in server answers the lookup within the bound")
                .expect_err("a consumer the ingestor cannot read fails the resume");
            let cause = error
                .downcast_ref::<NatsStreamIngestorError>()
                .assured("the typed cause stays beneath the resume failure");
            assert_eq!(cause.to_string(), expected);
            script.await.assured("the script answers the lookup");
        }
    }

    #[nervix_primitives::test]
    async fn each_delivery_is_acknowledged_or_negatively_acknowledged_on_its_own_subject() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 1).await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"first")
                .await;
            answer_settlement(&mut connection, "$JS.ACK.orders.nervix.1.1.1.0.0", b"+ACK").await;
            expect_pull(&mut connection, 1).await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.2.2.0.0", b"second")
                .await;
            answer_settlement(&mut connection, "$JS.ACK.orders.nervix.1.2.2.0.0", b"-NAK").await;
            connection
        });
        let mut source = opened(&plan).await;

        let first = read(&mut source, SEQUENTIAL).await;
        assert_eq!(payloads(&first), vec![b"first".to_vec()]);
        let positions = first
            .iter()
            .map(|message| message.position().clone())
            .collect::<Vec<_>>();
        timeout(OBSERVATION_BOUND, source.acknowledge(&positions))
            .await
            .assured("the server answers the acknowledgement within the bound")
            .assured("an answered acknowledgement confirms the delivery");

        let second = read(&mut source, SEQUENTIAL).await;
        assert_eq!(payloads(&second), vec![b"second".to_vec()]);
        let positions = second
            .iter()
            .map(|message| message.position().clone())
            .collect::<Vec<_>>();
        timeout(OBSERVATION_BOUND, source.reject(&positions))
            .await
            .assured("the server answers the negative acknowledgement within the bound")
            .assured("an answered negative acknowledgement returns the delivery");
        script
            .await
            .assured("the script follows the pulls and settlements");
    }

    #[nervix_primitives::test]
    async fn a_parallel_read_gathers_what_one_pull_brings_until_the_pull_ends() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 3).await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"first")
                .await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.2.2.0.0", b"second")
                .await;
            connection
                .deliver_status(&inbox, 408, "Request Timeout")
                .await;
            expect_pull(&mut connection, 3).await;
            connection
        });
        let mut source = opened(&plan).await;
        let parallel = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(3).assured("a fixed window of three is positive"),
            batch_timeout: Some(OBSERVATION_BOUND),
        };

        let batch = read(&mut source, parallel).await;
        assert_eq!(
            payloads(&batch),
            vec![b"first".to_vec(), b"second".to_vec()],
            "a pull that ended brings no more, so the read returns what it brought"
        );
        let cancellation = IngestorReadCancellation::new();
        let next = source.next_batch(parallel, &cancellation);
        let second_pull = timeout(OBSERVATION_BOUND, async {
            nervix_primitives::select! {
                _ = next => panic!("no delivery follows the second pull"),
                connection = script => connection,
            }
        })
        .await
        .assured("the next read asks again once the pull ended");
        drop(second_pull.assured("the script sees the second pull"));
    }

    /// A refusal that follows a delivery of the same pull fails the next read, once the read that
    /// gathered the delivery has handed it to the host.
    #[nervix_primitives::test]
    async fn a_refusal_while_gathering_fails_the_next_read_after_the_gathered_delivery() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 3).await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"first")
                .await;
            connection
                .deliver_status(&inbox, 409, "Consumer Deleted")
                .await;
            connection
        });
        let mut source = opened(&plan).await;
        let parallel = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(3).assured("a fixed window of three is positive"),
            batch_timeout: Some(OBSERVATION_BOUND),
        };

        let batch = read(&mut source, parallel).await;
        assert_eq!(
            payloads(&batch),
            vec![b"first".to_vec()],
            "the delivery the pull brought before its refusal reaches the host"
        );
        let next = timeout(
            OBSERVATION_BOUND,
            source.next_batch(parallel, &IngestorReadCancellation::new()),
        )
        .await
        .assured("the retained refusal returns at once");
        let Err(error) = next else {
            panic!("the next read reports the refusal the gathering read retained");
        };
        let cause = error
            .downcast_ref::<NatsStreamIngestorError>()
            .assured("the typed cause stays beneath the read failure");
        assert!(
            matches!(cause, NatsStreamIngestorError::PullRefused { .. }),
            "the retained failure is the refusal: {cause}"
        );
        assert!(source.needs_resume());
        script
            .await
            .assured("the script refuses the pull after its first delivery");
    }

    /// A parallel read whose pull brings fewer deliveries than it asked for returns them once its
    /// batch timeout passes. Its pull request stays outstanding, so the next read takes what that
    /// request brings later without asking again.
    #[nervix_primitives::test]
    async fn a_parallel_read_returns_what_arrived_once_its_batch_timeout_passes() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let (first_read, first_read_ended) = oneshot::channel::<()>();
        let (second_read, second_read_ended) = oneshot::channel::<()>();
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 3).await;
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"first")
                .await;
            first_read_ended
                .await
                .assured("the test ends its first read before the next delivery");
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.2.2.0.0", b"second")
                .await;
            second_read_ended
                .await
                .assured("the test ends its second read before the script checks it");
            connection.ping_client().await;
            connection
        });
        let mut source = opened(&plan).await;
        let parallel = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(3).assured("a fixed window of three is positive"),
            batch_timeout: Some(Duration::from_millis(50)),
        };

        let first = read(&mut source, parallel).await;
        assert_eq!(payloads(&first), vec![b"first".to_vec()]);
        first_read
            .send(())
            .assured("the script waits for the first read to end");
        let second = read(&mut source, parallel).await;
        assert_eq!(
            payloads(&second),
            vec![b"second".to_vec()],
            "the next read takes what the outstanding pull request brings"
        );
        second_read
            .send(())
            .assured("the script waits for the second read to end");
        script
            .await
            .assured("the client sent no second pull request while the first was outstanding");
    }

    #[nervix_primitives::test]
    async fn a_refused_pull_fails_the_read_and_leaves_the_instance_to_resume() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 1).await;
            connection
                .deliver_status(&inbox, 409, "Consumer Deleted")
                .await;
            connection
        });
        let mut source = opened(&plan).await;
        let read = timeout(
            OBSERVATION_BOUND,
            source.next_batch(SEQUENTIAL, &IngestorReadCancellation::new()),
        )
        .await
        .assured("the refusal reaches the instance within the bound");
        let Err(error) = read else {
            panic!("a refused pull fails the read");
        };
        assert!(matches!(
            error.current_context(),
            IngestorError::Read { connector: NATS }
        ));
        let cause = error
            .downcast_ref::<NatsStreamIngestorError>()
            .assured("the typed cause stays beneath the read failure");
        let NatsStreamIngestorError::PullRefused { refusal } = cause else {
            panic!("the server's status names the refusal: {cause}");
        };
        assert_eq!(
            *refusal,
            NatsPullRefusal {
                status: 409,
                description: Some("Consumer Deleted".to_owned()),
            }
        );
        assert!(source.needs_resume());
        script.await.assured("the script refuses the pull");
    }

    #[nervix_primitives::test]
    async fn a_cancelled_read_keeps_its_pull_and_the_next_read_takes_the_delivery() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let (allow_delivery, delivery_allowed) = oneshot::channel::<()>();
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 1).await;
            delivery_allowed
                .await
                .assured("the test allows the delivery once its read was cancelled");
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"held")
                .await;
            // The next publish is the delivery's acknowledgement, not a second pull request.
            answer_settlement(&mut connection, "$JS.ACK.orders.nervix.1.1.1.0.0", b"+ACK").await;
            connection
        });
        let mut source = opened(&plan).await;
        // A read whose host cancelled it sends its pull request and then ends at once.
        let cancellation = IngestorReadCancellation::new();
        cancellation.cancel();
        let interrupted = timeout(
            OBSERVATION_BOUND,
            source.next_batch(SEQUENTIAL, &cancellation),
        )
        .await
        .assured("a cancelled read ends at once")
        .assured("a cancelled read is not a failure");
        let IngestorBatch::Interrupted { gathered } = interrupted else {
            panic!("a read the host cancelled ends as interrupted");
        };
        assert!(gathered.is_empty(), "nothing was delivered to take");

        allow_delivery
            .send(())
            .assured("the script waits for the delivery to be allowed");
        let held = read(&mut source, SEQUENTIAL).await;
        assert_eq!(payloads(&held), vec![b"held".to_vec()]);
        let positions = held
            .iter()
            .map(|message| message.position().clone())
            .collect::<Vec<_>>();
        timeout(OBSERVATION_BOUND, source.acknowledge(&positions))
            .await
            .assured("the server answers the acknowledgement within the bound")
            .assured("an answered acknowledgement confirms the delivery");
        script
            .await
            .assured("the script follows the pull and its settlement");
    }

    #[nervix_primitives::test]
    async fn suspending_returns_a_delivery_the_client_received_to_the_consumer() {
        let server = ScriptedServer::start().await;
        let plan = plan(&server.addr());
        let (pulled, pull_sent) = oneshot::channel::<()>();
        let (ended, read_ended) = oneshot::channel::<()>();
        let (held, delivery_held) = oneshot::channel::<()>();
        let script = nervix_primitives::task::spawn(async move {
            let (mut connection, inbox) = bind(&server).await;
            expect_pull(&mut connection, 1).await;
            pulled
                .send(())
                .assured("the test waits for the pull before it cancels its read");
            read_ended
                .await
                .assured("the test ends its read before the delivery arrives");
            connection
                .deliver_for_reply(&inbox, "$JS.ACK.orders.nervix.1.1.1.0.0", b"received")
                .await;
            connection.ping_client().await;
            held.send(())
                .assured("the test suspends once the client holds the delivery");
            connection.expect_unsubscribe(&inbox).await;
            connection.answer_ping().await;
            let returned = connection.expect_publish().await;
            assert_eq!(returned.subject, "$JS.ACK.orders.nervix.1.1.1.0.0");
            assert_eq!(returned.message, b"-NAK");
            connection
        });
        let mut source = opened(&plan).await;
        let cancellation = IngestorReadCancellation::new();
        let cancelled_once_pulled = async {
            pull_sent.await.assured("the script sees the pull request");
            cancellation.cancel();
        };
        let (interrupted, ()) = timeout(
            OBSERVATION_BOUND,
            join(
                source.next_batch(SEQUENTIAL, &cancellation),
                cancelled_once_pulled,
            ),
        )
        .await
        .assured("a cancelled read ends at once");
        let interrupted = interrupted.assured("a cancelled read is not a failure");
        let IngestorBatch::Interrupted { gathered } = interrupted else {
            panic!("a read the host cancelled ends as interrupted");
        };
        assert!(
            gathered.is_empty(),
            "nothing was delivered before the read ended"
        );
        ended
            .send(())
            .assured("the script delivers once the read ended");
        timeout(OBSERVATION_BOUND, delivery_held)
            .await
            .assured("the client answers the script's ping within the bound")
            .assured("the script reports that the client holds the delivery");

        timeout(OBSERVATION_BOUND, source.suspend())
            .await
            .assured("the drain ends once the client wrote the unsubscription")
            .assured("suspending lets the pull go");
        assert!(source.needs_resume());
        script
            .await
            .assured("the script sees the received delivery returned");
    }

    #[nervix_primitives::test]
    async fn an_instance_without_a_pull_reads_nothing_and_settles_nothing() {
        let plan = plan("nats://127.0.0.1:4222");
        let mut source = NatsStreamIngestor::open(&plan, 0)
            .await
            .assured("opening a NATS STREAM ingestor connects nothing");
        let batch = source
            .next_batch(SEQUENTIAL, &IngestorReadCancellation::new())
            .await
            .assured("an instance without a pull asks to be resumed");
        assert!(matches!(batch, IngestorBatch::ResumeRequired));
        let position = NatsStreamIngestorPosition {
            acknowledgement: Subject::from("$JS.ACK.orders.nervix.1.1.1.0.0"),
        };
        let acknowledged = source
            .acknowledge(std::slice::from_ref(&position))
            .await
            .expect_err("no connection can confirm an acknowledgement");
        assert!(matches!(
            acknowledged.current_context(),
            IngestorError::Acknowledge { connector: NATS }
        ));
        let rejected = source
            .reject(std::slice::from_ref(&position))
            .await
            .expect_err("no connection can take a delivery back");
        assert!(matches!(
            rejected.current_context(),
            IngestorError::Reject { connector: NATS }
        ));
        for error in [acknowledged, rejected] {
            assert!(matches!(
                error.downcast_ref::<NatsStreamIngestorError>(),
                Some(NatsStreamIngestorError::Disconnected)
            ));
        }
    }
}
