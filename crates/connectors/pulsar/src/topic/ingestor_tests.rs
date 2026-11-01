//! The Pulsar ingestor's subscriptions, its reads that the host cancels, and its acknowledgements and
//! redelivery requests, against a broker on loopback.
//!
//! Each test drives the production ingestor through the client's real consumer, which subscribes to
//! a broker that delivers the messages a test asks for and answers each acknowledgement,
//! non-durable subscribe and unsubscribe as the test chooses, so an acknowledgement counts as
//! confirmed only once the broker answered it. The broker serves only its first connection: a
//! later one, such as a reconnection, is accepted and never answered, as a broker the client cannot
//! reach would leave it.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestorBatch, IngestorBatchRequest, IngestorConnector, IngestorError,
    IngestorMessage, IngestorReadCancellation, IngestorResume,
};
use nervix_models::{
    ClientConfigEntry, PulsarSubscription, PulsarSubscriptionName, PulsarSubscriptionOwnership,
    PulsarTopicName,
};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{Instant, sleep, timeout},
};
use pulsar::{
    Error as PulsarError, OperationRetryOptions, Pulsar, TokioExecutor,
    error::{ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError},
    message::{
        Codec as PulsarCodec, Message as PulsarFrame, Payload as PulsarPayload,
        proto::{
            BaseCommand, CommandAckResponse, CommandConnected, CommandError,
            CommandLookupTopicResponse, CommandMessage, CommandPartitionedTopicMetadataResponse,
            CommandPong, CommandSuccess, MessageIdData, MessageMetadata,
            base_command::Type as CommandType,
            command_lookup_topic_response::LookupType as TopicLookupType,
            command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
        },
    },
};
use tokio_util::codec::Framed;

use super::{
    DURABLE_SUBSCRIPTION_EXISTS, PulsarAckReceipts, PulsarRefusal, PulsarServerError,
    PulsarTopicIngestor, PulsarTopicIngestorError, PulsarTopicIngestorPlan,
    PulsarTopicIngestorPosition, PulsarTopicIngestorSettings, PulsarUnresolvedRequest,
};

const TOPIC: &str = "persistent://public/default/orders";

/// The client's operation timeout in these tests, which bounds every answer the consumer awaits.
const OPERATION_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a test waits for something the broker or the ingestor reports.
const DEADLINE: Duration = Duration::from_secs(30);

/// How the broker answers an acknowledgement.
#[derive(Clone, Copy, Debug)]
enum AckAnswer {
    /// The broker confirms it.
    Confirm,
    /// The broker refuses it with this server error and reason.
    Refuse(PulsarServerError, &'static str),
    /// The broker never answers it, as a broker does for a consumer it no longer knows.
    Ignore,
    /// The broker closes the connection that carried it.
    CloseConnection,
}

/// What the broker received from the consumer, in the order it arrived.
#[derive(Debug, PartialEq, Eq)]
enum Received {
    /// One acknowledgement command, and whether it carried a request id the broker could answer.
    Ack {
        message_ids: Vec<MessageIdData>,
        carried_request_id: bool,
    },
    /// One redelivery request.
    Redelivery { message_ids: Vec<MessageIdData> },
    /// A connection after the first, such as one a reconnection opened.
    Reconnection,
}

/// How the broker answers a subscribe command, or an unsubscribe.
#[derive(Clone, Copy, Debug)]
enum CommandAnswer {
    /// The broker carries it out.
    Accept,
    /// The broker refuses it with this server error and reason.
    Refuse(PulsarServerError, &'static str),
}

/// How the broker answers the subscription commands of the consumers it serves.
#[derive(Clone, Copy, Debug)]
struct SubscriptionAnswers {
    /// How it answers a non-durable subscribe, whose refusal can say that a durable subscription
    /// of the same name exists. A durable subscribe is always accepted.
    non_durable: CommandAnswer,
    unsubscribe: CommandAnswer,
}

impl SubscriptionAnswers {
    /// Answers for a subscription that does not exist and is removed when asked.
    const ABSENT: Self = Self {
        non_durable: CommandAnswer::Accept,
        unsubscribe: CommandAnswer::Accept,
    };
    /// Answers for a subscription an operator provisioned, as the broker refuses a non-durable
    /// subscribe of its name.
    const PROVISIONED: Self = Self {
        non_durable: CommandAnswer::Refuse(
            PulsarServerError::NotAllowedError,
            "Durable subscription with the same name already exists.",
        ),
        unsubscribe: CommandAnswer::Accept,
    };
}

/// A subscription command the broker received, in the order consumers sent them.
#[derive(Debug, PartialEq, Eq)]
enum SubscriptionCommand {
    Subscribe {
        durable: bool,
        consumer_name: Option<String>,
    },
    Unsubscribe,
    CloseConsumer,
}

/// What the broker tells a test it received, on two channels: the acknowledgements and redelivery
/// requests of the consumer it serves, and the subscription commands of every consumer.
struct Observed {
    received: mpsc::UnboundedSender<Received>,
    subscriptions: mpsc::UnboundedSender<SubscriptionCommand>,
}

/// The message a loopback delivery carries before a real consumer id is selected.
struct Delivery {
    entry_id: u64,
    redelivery_count: Option<u32>,
    metadata: MessageMetadata,
}

impl Delivery {
    fn standard(entry_id: u64) -> Self {
        Self {
            entry_id,
            redelivery_count: Some(0),
            metadata: MessageMetadata {
                producer_name: "loopback-producer".to_owned(),
                sequence_id: entry_id,
                publish_time: 1,
                ..Default::default()
            },
        }
    }

    fn frame(&self, consumer_id: u64) -> PulsarFrame {
        PulsarFrame {
            command: BaseCommand {
                r#type: i32::from(CommandType::Message),
                message: Some(CommandMessage {
                    consumer_id,
                    message_id: message_id(self.entry_id),
                    redelivery_count: self.redelivery_count,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: Some(PulsarPayload {
                metadata: self.metadata.clone(),
                data: format!("{{\"user_id\":{}}}", self.entry_id).into_bytes(),
            }),
        }
    }
}

/// A Pulsar broker on loopback that serves shared-subscription consumers: it delivers `messages`
/// once the first durable consumer asks for them, answers acknowledgements with `answer` and
/// subscription commands with `subscriptions`, and reports what it receives.
struct LoopbackBroker {
    service_url: String,
    received: mpsc::UnboundedReceiver<Received>,
    subscriptions: mpsc::UnboundedReceiver<SubscriptionCommand>,
}

impl LoopbackBroker {
    async fn start(messages: u64, answer: AckAnswer) -> Self {
        Self::start_with(messages, answer, SubscriptionAnswers::ABSENT).await
    }

    async fn start_with(
        messages: u64,
        answer: AckAnswer,
        subscriptions: SubscriptionAnswers,
    ) -> Self {
        let deliveries = (0..messages).map(Delivery::standard).collect();
        Self::start_deliveries(deliveries, answer, subscriptions).await
    }

    async fn start_deliveries(
        deliveries: Vec<Delivery>,
        answer: AckAnswer,
        subscriptions: SubscriptionAnswers,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        let addr = listener
            .local_addr()
            .expect("a bound listener has an address");
        let service_url = format!("pulsar://{addr}");
        let (received_tx, received) = mpsc::unbounded_channel();
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded_channel();
        let lookup_url = service_url.clone();
        nervix_primitives::task::spawn(async move {
            let Ok((first, _)) = listener.accept().await else {
                return;
            };
            let observed = Observed {
                received: received_tx.clone(),
                subscriptions: subscriptions_tx,
            };
            nervix_primitives::task::spawn(Self::serve(
                Framed::new(first, PulsarCodec),
                lookup_url,
                deliveries,
                answer,
                subscriptions,
                observed,
            ));
            loop {
                nervix_primitives::task::consume_budget().await;
                let Ok((later, _)) = listener.accept().await else {
                    return;
                };
                if received_tx.send(Received::Reconnection).is_err() {
                    return;
                }
                nervix_primitives::task::spawn(Self::hold_unanswered(later));
            }
        });
        Self {
            service_url,
            received,
            subscriptions: subscriptions_rx,
        }
    }

    async fn serve(
        mut connection: Framed<TcpStream, PulsarCodec>,
        service_url: String,
        deliveries: Vec<Delivery>,
        answer: AckAnswer,
        subscriptions: SubscriptionAnswers,
        observed: Observed,
    ) {
        let mut delivered = false;
        // Only a durable consumer reads the messages: a non-durable probe is closed unread.
        let mut durable_consumers = std::collections::BTreeSet::new();
        while let Some(Ok(frame)) = connection.next().await {
            nervix_primitives::task::consume_budget().await;
            let command = frame.command;
            let mut replies = Vec::new();
            if command.connect.is_some() {
                replies.push(Self::connected());
            } else if let Some(request) = command.partition_metadata {
                replies.push(Self::unpartitioned(request.request_id));
            } else if let Some(request) = command.lookup_topic {
                replies.push(Self::served_here(&service_url, request.request_id));
            } else if let Some(request) = command.subscribe {
                let durable = request.durable.unwrap_or(true);
                let subscribed = SubscriptionCommand::Subscribe {
                    durable,
                    consumer_name: request.consumer_name,
                };
                if observed.subscriptions.send(subscribed).is_err() {
                    return;
                }
                let answer = if durable {
                    durable_consumers.insert(request.consumer_id);
                    CommandAnswer::Accept
                } else {
                    subscriptions.non_durable
                };
                replies.push(Self::answer(request.request_id, answer));
            } else if let Some(request) = command.unsubscribe {
                if observed
                    .subscriptions
                    .send(SubscriptionCommand::Unsubscribe)
                    .is_err()
                {
                    return;
                }
                replies.push(Self::answer(request.request_id, subscriptions.unsubscribe));
            } else if let Some(flow) = command.flow {
                if !delivered && durable_consumers.contains(&flow.consumer_id) {
                    delivered = true;
                    replies.extend(
                        deliveries
                            .iter()
                            .map(|delivery| delivery.frame(flow.consumer_id)),
                    );
                }
            } else if let Some(ack) = command.ack {
                let acknowledged = Received::Ack {
                    message_ids: ack.message_id,
                    carried_request_id: ack.request_id.is_some(),
                };
                if observed.received.send(acknowledged).is_err() {
                    return;
                }
                // Only an acknowledgement that carried a request id can be answered.
                match (answer, ack.request_id) {
                    (AckAnswer::Confirm, Some(request_id)) => {
                        replies.push(Self::ack_confirmation(ack.consumer_id, request_id));
                    }
                    (AckAnswer::Refuse(error, reason), Some(request_id)) => {
                        replies.push(Self::ack_refusal(
                            ack.consumer_id,
                            request_id,
                            error,
                            reason,
                        ));
                    }
                    (AckAnswer::CloseConnection, _) => return,
                    (AckAnswer::Confirm | AckAnswer::Refuse(..) | AckAnswer::Ignore, _) => {}
                }
            } else if let Some(redelivery) = command.redeliver_unacknowledged_messages {
                let requested = Received::Redelivery {
                    message_ids: redelivery.message_ids,
                };
                if observed.received.send(requested).is_err() {
                    return;
                }
            } else if let Some(request) = command.close_consumer {
                if observed
                    .subscriptions
                    .send(SubscriptionCommand::CloseConsumer)
                    .is_err()
                {
                    return;
                }
                replies.push(Self::success(request.request_id));
            } else if command.ping.is_some() {
                replies.push(Self::pong());
            }
            for reply in replies {
                if connection.send(reply).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Reads what a later connection writes and never answers it, so the client's handshake on it
    /// waits for its connection timeout.
    async fn hold_unanswered(connection: TcpStream) {
        let mut connection = Framed::new(connection, PulsarCodec);
        while let Some(Ok(_)) = connection.next().await {
            nervix_primitives::task::consume_budget().await;
        }
    }

    fn frame(command: BaseCommand) -> PulsarFrame {
        PulsarFrame {
            command,
            payload: None,
        }
    }

    fn connected() -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::Connected),
            connected: Some(CommandConnected {
                server_version: "loopback".to_string(),
                protocol_version: Some(12),
                max_message_size: None,
            }),
            ..Default::default()
        })
    }

    fn unpartitioned(request_id: u64) -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::PartitionedMetadataResponse),
            partition_metadata_response: Some(CommandPartitionedTopicMetadataResponse {
                partitions: Some(0),
                request_id,
                response: Some(i32::from(PartitionLookupType::Success)),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn served_here(service_url: &str, request_id: u64) -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::LookupResponse),
            lookup_topic_response: Some(CommandLookupTopicResponse {
                broker_service_url: Some(service_url.to_string()),
                response: Some(i32::from(TopicLookupType::Connect)),
                request_id,
                authoritative: Some(true),
                proxy_through_service_url: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn success(request_id: u64) -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::Success),
            success: Some(CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        })
    }

    /// The broker's answer to the request `request_id`: its success, or the refusal `answer`
    /// names.
    fn answer(request_id: u64, answer: CommandAnswer) -> PulsarFrame {
        let CommandAnswer::Refuse(error, reason) = answer else {
            return Self::success(request_id);
        };
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::Error),
            error: Some(CommandError {
                request_id,
                error: i32::from(error),
                message: reason.to_string(),
            }),
            ..Default::default()
        })
    }

    fn ack_confirmation(consumer_id: u64, request_id: u64) -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::AckResponse),
            ack_response: Some(CommandAckResponse {
                consumer_id,
                request_id: Some(request_id),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn ack_refusal(
        consumer_id: u64,
        request_id: u64,
        error: PulsarServerError,
        reason: &str,
    ) -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::AckResponse),
            ack_response: Some(CommandAckResponse {
                consumer_id,
                error: Some(i32::from(error)),
                message: Some(reason.to_string()),
                request_id: Some(request_id),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn pong() -> PulsarFrame {
        Self::frame(BaseCommand {
            r#type: i32::from(CommandType::Pong),
            pong: Some(CommandPong {}),
            ..Default::default()
        })
    }

    /// The next thing the broker received from the consumer.
    async fn next_received(&mut self) -> Received {
        timeout(DEADLINE, self.received.recv())
            .await
            .expect("the broker receives the consumer's next command")
            .expect("the broker keeps listening while the test runs")
    }

    /// The next subscription command the broker received from any consumer.
    async fn next_subscription_command(&mut self) -> SubscriptionCommand {
        timeout(DEADLINE, self.subscriptions.recv())
            .await
            .expect("the broker receives the next subscription command")
            .expect("the broker keeps listening while the test runs")
    }

    /// The client entries of an ingestor that reads through this broker.
    fn config(&self) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: self.service_url.clone(),
        }]
    }
}

/// The id of the loopback broker's message at `entry_id`.
fn message_id(entry_id: u64) -> MessageIdData {
    MessageIdData {
        ledger_id: 7,
        entry_id,
        ..Default::default()
    }
}

/// The ingestor of one instance under an `ACK` mode, subscribed on `broker` through a client whose
/// operation timeout is short enough for a test to outlast.
async fn open(broker: &LoopbackBroker) -> PulsarTopicIngestor {
    open_with(broker, PulsarAckReceipts::Awaited).await
}

async fn open_with(
    broker: &LoopbackBroker,
    ack_receipts: PulsarAckReceipts,
) -> PulsarTopicIngestor {
    let plan = plan(
        broker,
        ack_receipts,
        PulsarSubscriptionOwnership::Ingestor {
            start_at: nervix_models::PulsarStartPosition::Earliest,
        },
    )
    .await;
    PulsarTopicIngestor::open(&plan, 0)
        .await
        .expect("the loopback broker serves the subscription")
}

/// The plan of an ingestor on `broker` whose subscription `ownership` says who owns, through a client
/// whose operation timeout is short enough for a test to outlast.
async fn plan(
    broker: &LoopbackBroker,
    ack_receipts: PulsarAckReceipts,
    ownership: PulsarSubscriptionOwnership,
) -> PulsarTopicIngestorPlan {
    let client = Pulsar::builder(broker.service_url.clone(), TokioExecutor)
        .with_operation_retry_options(OperationRetryOptions {
            operation_timeout: OPERATION_TIMEOUT,
            ..Default::default()
        })
        .build()
        .await
        .expect("the loopback broker serves the client's handshake");
    PulsarTopicIngestorPlan {
        client,
        topic: TOPIC.to_string(),
        subscription: subscription(ownership),
        consumer_name: "orders".to_string(),
        ack_receipts,
    }
}

/// The subscription every test ingestor reads through, owned as `ownership` says.
fn subscription(ownership: PulsarSubscriptionOwnership) -> PulsarSubscription {
    PulsarSubscription {
        name: PulsarSubscriptionName::parse("orders-ingestor")
            .expect("the test subscription is a valid Pulsar subscription name"),
        ownership,
    }
}

/// The topic every test ingestor reads.
fn topic() -> PulsarTopicName {
    PulsarTopicName::parse(TOPIC).expect("the test topic is a valid Pulsar topic name")
}

/// The settings that remove the owned subscription of an ingestor on the broker `config` names.
fn removal_settings<'a>(
    config: &'a [ClientConfigEntry],
    topic: &'a PulsarTopicName,
) -> PulsarTopicIngestorSettings<'a> {
    PulsarTopicIngestorSettings {
        config,
        topic,
        subscription: subscription(PulsarSubscriptionOwnership::Ingestor {
            start_at: nervix_models::PulsarStartPosition::Earliest,
        }),
        consumer_name: "orders".to_string(),
        ack_receipts: PulsarAckReceipts::Awaited,
    }
}

/// The positions of the first `count` messages the ingestor reads.
async fn read(source: &mut PulsarTopicIngestor, count: usize) -> Vec<PulsarTopicIngestorPosition> {
    let mut positions = Vec::new();
    while positions.len() < count {
        let request = IngestorBatchRequest {
            max_messages: std::num::NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let batch = timeout(
            DEADLINE,
            source.next_batch(request, &IngestorReadCancellation::new()),
        )
        .await
        .expect("the ingestor reads the broker's next message")
        .expect("the loopback broker delivers its messages");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("an open ingestor reads messages");
        };
        positions.extend(messages.iter().map(|message| message.position().clone()));
    }
    positions
}

/// How long a test lets the consumer receive before it reads again.
const RECEIVE_INTERVAL: Duration = Duration::from_millis(10);

#[nervix_primitives::test]
async fn broker_redelivery_counts_reach_the_message_metadata_with_the_protocol_default() {
    use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};

    let counts = [None, Some(0), Some(1), Some(u32::MAX)];
    let mut deliveries = Vec::new();
    for (index, count) in counts.into_iter().enumerate() {
        let entry = u64::try_from(index).assured("the four fixture deliveries fit u64");
        let mut delivery = Delivery::standard(entry);
        delivery.redelivery_count = count;
        deliveries.push(delivery);
    }
    let broker = LoopbackBroker::start_deliveries(
        deliveries,
        AckAnswer::Confirm,
        SubscriptionAnswers::ABSENT,
    )
    .await;
    let mut source = open(&broker).await;
    for count in counts {
        nervix_primitives::task::consume_budget().await;
        let batch = timeout(
            DEADLINE,
            source.next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            ),
        )
        .await
        .assured("the loopback broker delivers the selected packet")
        .assured("each selected packet has representable metadata");
        let IngestorBatch::Messages(messages) = batch else {
            panic!("the packet must reach its message owner");
        };
        assert_eq!(messages.len(), 1);
        let captured = RetainedIngestMetadata::capture(messages[0].metadata().fields);
        let expected = match count {
            Some(count) => Value::I64(i64::from(count)),
            None => Value::I64(0),
        };
        assert_eq!(captured.values().get(8), Some(&expected));
    }
}

#[nervix_primitives::test]
async fn metadata_failure_returns_the_gathered_prefix_and_survives_a_cancelled_read() {
    let mut invalid = Delivery::standard(1);
    invalid.metadata.publish_time = u64::MAX;
    let deliveries = vec![Delivery::standard(0), invalid, Delivery::standard(2)];
    let broker = LoopbackBroker::start_deliveries(
        deliveries,
        AckAnswer::Confirm,
        SubscriptionAnswers::ABSENT,
    )
    .await;
    let mut source = open(&broker).await;
    let request = IngestorBatchRequest {
        max_messages: std::num::NonZeroUsize::new(3).assured("three is nonzero"),
        batch_timeout: Some(Duration::from_secs(1)),
    };
    let first = timeout(
        DEADLINE,
        source.next_batch(request, &IngestorReadCancellation::new()),
    )
    .await
    .assured("the broker sends the entire fixture")
    .assured("the valid prefix must be returned before the metadata failure");
    let IngestorBatch::Messages(messages) = first else {
        panic!("the first read must return its prefix");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload(), br#"{"user_id":0}"#);
    let cancelled = IngestorReadCancellation::new();
    cancelled.cancel();
    let interrupted = source
        .next_batch(request, &cancelled)
        .await
        .assured("cancellation keeps the deferred failure");
    assert!(matches!(interrupted, IngestorBatch::Interrupted { gathered } if gathered.is_empty()));
    let Err(error) = source
        .next_batch(request, &IngestorReadCancellation::new())
        .await
    else {
        panic!("the exact metadata failure must remain pending after cancellation");
    };
    assert!(error.contains::<nervix_models::TimestampError>());
    assert!(matches!(
        error.current_context(),
        IngestorError::Read { .. }
    ));
    let next = timeout(
        DEADLINE,
        source.next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        ),
    )
    .await
    .assured("the next current packet remains readable")
    .assured("the metadata failure is reported exactly once");
    let IngestorBatch::Messages(messages) = next else {
        panic!("the next packet must remain a message");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload(), br#"{"user_id":2}"#);
}

#[nervix_primitives::test]
async fn a_cancelled_parallel_read_returns_every_message_it_gathered() {
    let broker = LoopbackBroker::start(4, AckAnswer::Confirm).await;
    let mut source = open(&broker).await;
    // The first read waits until the broker has delivered its messages.
    let mut entries = Vec::new();
    for position in read(&mut source, 1).await {
        entries.push(position.message_id.entry_id);
    }

    // A read the host has already cancelled takes only what the consumer holds and then ends, so
    // the test reads again until the consumer has received every message.
    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    let request = IngestorBatchRequest {
        max_messages: std::num::NonZeroUsize::new(8).expect("eight is a nonzero batch bound"),
        batch_timeout: Some(Duration::from_secs(3600)),
    };
    let deadline = Instant::now()
        .checked_add(DEADLINE)
        .expect("thirty seconds after now fits the monotonic clock");
    while entries.len() < 4 {
        let batch = source
            .next_batch(request, &cancellation)
            .await
            .expect("the loopback broker delivers its messages");
        let IngestorBatch::Interrupted { gathered } = batch else {
            panic!("a read the host cancelled ends as interrupted");
        };
        if gathered.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the consumer received the delivered messages within the deadline"
            );
            sleep(RECEIVE_INTERVAL).await;
            continue;
        }
        for message in &gathered {
            entries.push(message.position().message_id.entry_id);
        }
    }

    assert_eq!(
        entries,
        [0, 1, 2, 3],
        "the interrupted reads returned every message they took, each exactly once"
    );
}

/// The Pulsar failure an ingestor operation reports beneath its connector context.
fn pulsar_failure(error: &error_stack::Report<IngestorError>) -> &PulsarTopicIngestorError {
    error
        .downcast_ref::<PulsarTopicIngestorError>()
        .expect("a Pulsar ingestor failure carries its Pulsar cause")
}

#[nervix_primitives::test]
async fn acknowledging_returns_once_the_broker_confirmed_the_acknowledgement() {
    let mut broker = LoopbackBroker::start(1, AckAnswer::Confirm).await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 1).await;

    source
        .acknowledge(&positions)
        .await
        .expect("the broker confirms the acknowledgement");

    assert_eq!(
        broker.next_received().await,
        Received::Ack {
            message_ids: vec![message_id(0)],
            carried_request_id: true,
        }
    );
}

#[nervix_primitives::test]
async fn the_positions_of_one_topic_are_confirmed_by_one_acknowledgement() {
    let mut broker = LoopbackBroker::start(3, AckAnswer::Confirm).await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 3).await;

    source
        .acknowledge(&positions)
        .await
        .expect("the broker confirms the acknowledgement");

    assert_eq!(
        broker.next_received().await,
        Received::Ack {
            message_ids: vec![message_id(0), message_id(1), message_id(2)],
            carried_request_id: true,
        }
    );
}

#[nervix_primitives::test]
async fn an_acknowledgement_the_broker_refuses_reports_its_server_error_and_reason() {
    let broker = LoopbackBroker::start(
        1,
        AckAnswer::Refuse(
            PulsarServerError::NotAllowedError,
            "the subscription is being deleted",
        ),
    )
    .await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 1).await;

    let error = source
        .acknowledge(&positions)
        .await
        .expect_err("the broker refuses the acknowledgement");

    let PulsarTopicIngestorError::AcknowledgementRefused { refusal } = pulsar_failure(&error)
    else {
        panic!("a refused acknowledgement names the refusal, got {error:?}");
    };
    assert_eq!(
        refusal,
        &PulsarRefusal {
            error: Some(PulsarServerError::NotAllowedError),
            reason: Some("the subscription is being deleted".to_string()),
        }
    );
    assert!(matches!(
        error.current_context(),
        IngestorError::Acknowledge {
            connector: "pulsar"
        }
    ));
}

#[nervix_primitives::test]
async fn an_acknowledgement_the_broker_does_not_answer_is_unconfirmed_after_the_operation_timeout()
{
    let broker = LoopbackBroker::start(1, AckAnswer::Ignore).await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 1).await;

    let error = source
        .acknowledge(&positions)
        .await
        .expect_err("an unanswered acknowledgement is not confirmed");

    assert!(
        matches!(
            pulsar_failure(&error),
            PulsarTopicIngestorError::AcknowledgementUnconfirmed {
                unresolved: PulsarUnresolvedRequest::OperationTimeout
            }
        ),
        "an unanswered acknowledgement waits out the operation timeout, got {error:?}"
    );
}

#[nervix_primitives::test]
async fn a_redelivery_request_the_consumer_cannot_send_is_reported_after_its_connection_closed() {
    let mut broker = LoopbackBroker::start(1, AckAnswer::CloseConnection).await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 1).await;

    let error = source
        .acknowledge(&positions)
        .await
        .expect_err("an acknowledgement whose connection closed is not confirmed");
    assert!(
        matches!(
            pulsar_failure(&error),
            PulsarTopicIngestorError::AcknowledgementUnconfirmed {
                unresolved: PulsarUnresolvedRequest::ConnectionClosed
            }
        ),
        "the closed connection leaves the acknowledgement unconfirmed, got {error:?}"
    );
    assert!(matches!(
        broker.next_received().await,
        Received::Ack {
            carried_request_id: true,
            ..
        }
    ));
    // The consumer reconnects to a broker that never answers the new connection, which holds every
    // request it was asked to send behind the reconnection.
    assert_eq!(broker.next_received().await, Received::Reconnection);

    let error = source
        .reject(&positions)
        .await
        .expect_err("a consumer that cannot reach its broker does not send the request");

    assert!(
        matches!(
            pulsar_failure(&error),
            PulsarTopicIngestorError::RedeliveryRequestUnsent {
                unresolved: PulsarUnresolvedRequest::OperationTimeout
            }
        ),
        "the reconnection holds the redelivery request past the operation timeout, got {error:?}"
    );
    assert!(matches!(
        error.current_context(),
        IngestorError::Reject {
            connector: "pulsar"
        }
    ));
}

#[nervix_primitives::test]
async fn a_mode_without_an_acknowledgement_tree_does_not_wait_for_the_broker() {
    let mut broker = LoopbackBroker::start(1, AckAnswer::Ignore).await;
    let mut source = open_with(&broker, PulsarAckReceipts::Unawaited).await;
    let positions = read(&mut source, 1).await;

    source
        .acknowledge(&positions)
        .await
        .expect("an acknowledgement the broker never answers returns once queued");

    assert_eq!(
        broker.next_received().await,
        Received::Ack {
            message_ids: vec![message_id(0)],
            carried_request_id: false,
        }
    );
}

#[nervix_primitives::test]
async fn rejecting_sends_a_redelivery_request_for_the_rejected_message() {
    let mut broker = LoopbackBroker::start(1, AckAnswer::Confirm).await;
    let mut source = open(&broker).await;
    let positions = read(&mut source, 1).await;

    source
        .reject(&positions)
        .await
        .expect("the consumer sends the redelivery request");

    assert_eq!(
        broker.next_received().await,
        Received::Redelivery {
            message_ids: vec![message_id(0)],
        }
    );
}

#[test]
fn a_client_failure_is_classified_by_what_it_says_about_the_request() {
    let timed_out = PulsarConsumerError::Connection(PulsarConnectionError::Io(
        std::io::Error::new(std::io::ErrorKind::TimedOut, "no answer"),
    ));
    assert_eq!(
        PulsarUnresolvedRequest::of(&timed_out),
        Some(PulsarUnresolvedRequest::OperationTimeout)
    );
    for closed in [
        PulsarConnectionError::Disconnected,
        PulsarConnectionError::Canceled,
        PulsarConnectionError::Shutdown,
    ] {
        assert_eq!(
            PulsarUnresolvedRequest::of(&PulsarConsumerError::Connection(closed)),
            Some(PulsarUnresolvedRequest::ConnectionClosed)
        );
    }
    assert_eq!(
        PulsarUnresolvedRequest::of(&PulsarConsumerError::Closed),
        Some(PulsarUnresolvedRequest::ConsumerStopped)
    );
    let refused = PulsarConsumerError::Connection(PulsarConnectionError::PulsarError(
        Some(PulsarServerError::NotAllowedError),
        Some("fenced".to_string()),
    ));
    assert_eq!(PulsarUnresolvedRequest::of(&refused), None);
    let failed_io = PulsarConsumerError::Connection(PulsarConnectionError::Io(
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken"),
    ));
    assert_eq!(PulsarUnresolvedRequest::of(&failed_io), None);
}

#[test]
fn a_failure_the_source_does_not_classify_keeps_the_client_error_beneath_it() {
    let unclassified = || {
        PulsarConsumerError::Connection(PulsarConnectionError::Unexpected(
            "a response the client cannot read".to_string(),
        ))
    };

    let acknowledgement = PulsarTopicIngestorError::unsettled_acknowledgement(unclassified());
    assert!(matches!(
        acknowledgement.current_context(),
        PulsarTopicIngestorError::Acknowledge
    ));
    assert!(acknowledgement.contains::<PulsarConsumerError>());

    let redelivery = PulsarTopicIngestorError::unsent_redelivery_request(unclassified());
    assert!(matches!(
        redelivery.current_context(),
        PulsarTopicIngestorError::NegativelyAcknowledge
    ));
    assert!(redelivery.contains::<PulsarConsumerError>());

    // The broker answers no redelivery request, so a refusal can only come from an
    // acknowledgement.
    let refused = PulsarConsumerError::Connection(PulsarConnectionError::PulsarError(
        Some(PulsarServerError::ServiceNotReady),
        None,
    ));
    assert!(matches!(
        PulsarTopicIngestorError::unsent_redelivery_request(refused).current_context(),
        PulsarTopicIngestorError::NegativelyAcknowledge
    ));
}

#[test]
fn a_refusal_names_the_server_error_and_the_reason_the_broker_gave() {
    let refusal = PulsarRefusal {
        error: Some(PulsarServerError::NotAllowedError),
        reason: Some("the subscription is being deleted".to_string()),
    };
    assert_eq!(
        refusal.to_string(),
        "NotAllowedError: the subscription is being deleted"
    );
    let unknown = PulsarRefusal {
        error: None,
        reason: None,
    };
    assert_eq!(
        unknown.to_string(),
        "a server error this client does not know"
    );
    assert_eq!(
        PulsarTopicIngestorError::AcknowledgementUnconfirmed {
            unresolved: PulsarUnresolvedRequest::OperationTimeout
        }
        .to_string(),
        "the Pulsar broker did not confirm an acknowledgement: the client's operation timeout \
         passed first"
    );
}

/// Whether `error` holds the Pulsar failure `matches` accepts in any of its frames.
fn holds_pulsar_failure(
    error: &error_stack::Report<PulsarTopicIngestorError>,
    matches: impl Fn(&PulsarTopicIngestorError) -> bool,
) -> bool {
    error
        .frames()
        .filter_map(|frame| frame.downcast_ref::<PulsarTopicIngestorError>())
        .any(matches)
}

#[nervix_primitives::test]
async fn an_instance_joins_its_owned_subscription_at_open_under_its_indexed_consumer_name() {
    let mut broker = LoopbackBroker::start(0, AckAnswer::Confirm).await;
    let plan = plan(
        &broker,
        PulsarAckReceipts::Awaited,
        PulsarSubscriptionOwnership::Ingestor {
            start_at: nervix_models::PulsarStartPosition::Earliest,
        },
    )
    .await;

    let mut source = PulsarTopicIngestor::open(&plan, 1)
        .await
        .expect("the loopback broker serves the subscription");

    assert!(
        !source.needs_resume(),
        "an owned subscription is joined at open"
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: true,
            consumer_name: Some("orders-1".to_string()),
        }
    );
}

#[nervix_primitives::test]
async fn a_provisioned_subscription_is_verified_then_joined_on_resume() {
    let mut broker =
        LoopbackBroker::start_with(1, AckAnswer::Confirm, SubscriptionAnswers::PROVISIONED).await;
    let plan = plan(
        &broker,
        PulsarAckReceipts::Awaited,
        PulsarSubscriptionOwnership::Provisioned,
    )
    .await;
    let mut source = PulsarTopicIngestor::open(&plan, 0)
        .await
        .expect("opening a provisioned subscription asks the broker nothing");
    assert!(
        source.needs_resume(),
        "a provisioned subscription is joined on resume"
    );

    let resumed = source
        .resume()
        .await
        .expect("the broker refuses the probe because the subscription exists");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: false,
            consumer_name: Some("orders-0".to_string()),
        }
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: true,
            consumer_name: Some("orders-0".to_string()),
        }
    );
    assert_eq!(read(&mut source, 1).await.len(), 1);
}

#[nervix_primitives::test]
async fn a_missing_provisioned_subscription_is_reported_and_never_created() {
    let mut broker =
        LoopbackBroker::start_with(1, AckAnswer::Confirm, SubscriptionAnswers::ABSENT).await;
    let plan = plan(
        &broker,
        PulsarAckReceipts::Awaited,
        PulsarSubscriptionOwnership::Provisioned,
    )
    .await;
    let mut source = PulsarTopicIngestor::open(&plan, 0)
        .await
        .expect("opening a provisioned subscription asks the broker nothing");

    let error = source
        .resume()
        .await
        .expect_err("a subscription nobody provisioned is missing");

    let PulsarTopicIngestorError::MissingSubscription {
        subscription,
        topic,
    } = pulsar_failure(&error)
    else {
        panic!("a missing subscription is named, got {error:?}");
    };
    assert_eq!(subscription.as_str(), "orders-ingestor");
    assert_eq!(topic, TOPIC);
    assert!(matches!(
        error.current_context(),
        IngestorError::Resume {
            connector: "pulsar"
        }
    ));
    assert_eq!(
        pulsar_failure(&error).to_string(),
        "PULSAR subscription 'orders-ingestor' of topic 'persistent://public/default/orders' does \
         not exist"
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: false,
            consumer_name: Some("orders-0".to_string()),
        }
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::CloseConsumer
    );
    assert!(
        source.needs_resume(),
        "the ingestor joins no subscription it would create"
    );
}

#[nervix_primitives::test]
async fn a_probe_refused_for_another_reason_does_not_say_the_subscription_exists() {
    let answers = SubscriptionAnswers {
        non_durable: CommandAnswer::Refuse(
            PulsarServerError::NotAllowedError,
            "Exceed the maximum number of subscriptions of the topic: \
             persistent://public/default/orders",
        ),
        unsubscribe: CommandAnswer::Accept,
    };
    let mut broker = LoopbackBroker::start_with(0, AckAnswer::Confirm, answers).await;
    let plan = plan(
        &broker,
        PulsarAckReceipts::Awaited,
        PulsarSubscriptionOwnership::Provisioned,
    )
    .await;
    let mut source = PulsarTopicIngestor::open(&plan, 0)
        .await
        .expect("opening a provisioned subscription asks the broker nothing");

    let error = source
        .resume()
        .await
        .expect_err("a refused probe leaves the subscription unverified");

    assert!(
        matches!(
            pulsar_failure(&error),
            PulsarTopicIngestorError::VerifySubscription { .. }
        ),
        "the refusal is a verification failure, got {error:?}"
    );
    assert!(
        error.contains::<PulsarError>(),
        "the refusal stays beneath the failure"
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: false,
            consumer_name: Some("orders-0".to_string()),
        }
    );
    assert!(source.needs_resume());
}

#[nervix_primitives::test]
async fn removing_an_owned_subscription_joins_it_and_unsubscribes() {
    let mut broker = LoopbackBroker::start(0, AckAnswer::Confirm).await;
    let config = broker.config();
    let topic = topic();

    PulsarTopicIngestorPlan::remove_subscription_within(
        removal_settings(&config, &topic),
        DEADLINE,
    )
    .await
    .expect("the broker removes the subscription");

    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: true,
            consumer_name: Some("orders-removal".to_string()),
        }
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Unsubscribe
    );
}

#[nervix_primitives::test]
async fn an_unsubscribe_the_broker_refuses_is_reported_and_closes_the_joined_consumer() {
    let answers = SubscriptionAnswers {
        non_durable: CommandAnswer::Accept,
        unsubscribe: CommandAnswer::Refuse(
            PulsarServerError::MetadataError,
            "Unconnected or shared consumer attempting to unsubscribe",
        ),
    };
    let mut broker = LoopbackBroker::start_with(0, AckAnswer::Confirm, answers).await;
    let config = broker.config();
    let topic = topic();

    let error = PulsarTopicIngestorPlan::remove_subscription_within(
        removal_settings(&config, &topic),
        DEADLINE,
    )
    .await
    .expect_err("the broker refuses the unsubscribe");

    let PulsarTopicIngestorError::RemoveSubscription {
        subscription,
        topic,
    } = error.current_context()
    else {
        panic!("a failed removal names its subscription, got {error:?}");
    };
    assert_eq!(subscription.as_str(), "orders-ingestor");
    assert_eq!(topic, TOPIC);
    assert!(
        holds_pulsar_failure(&error, |failure| matches!(
            failure,
            PulsarTopicIngestorError::Unsubscribe
        )),
        "the refused unsubscribe is beneath the removal, got {error:?}"
    );
    assert!(
        error.contains::<PulsarError>(),
        "the refusal stays beneath the failure"
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Subscribe {
            durable: true,
            consumer_name: Some("orders-removal".to_string()),
        }
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::Unsubscribe
    );
    assert_eq!(
        broker.next_subscription_command().await,
        SubscriptionCommand::CloseConsumer
    );
}

#[nervix_primitives::test]
async fn a_removal_the_broker_never_answers_ends_at_its_budget() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is available");
    let addr = listener
        .local_addr()
        .expect("a bound listener has an address");
    nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((connection, _)) = listener.accept().await else {
                return;
            };
            nervix_primitives::task::spawn(LoopbackBroker::hold_unanswered(connection));
        }
    });
    let config = vec![ClientConfigEntry {
        key: "addr".to_string(),
        value: format!("pulsar://{addr}"),
    }];
    let topic = topic();
    let budget = Duration::from_millis(200);
    let started = Instant::now();

    let error = PulsarTopicIngestorPlan::remove_subscription_within(
        removal_settings(&config, &topic),
        budget,
    )
    .await
    .expect_err("a broker that never answers removes nothing");

    assert!(started.elapsed() >= budget);
    assert!(matches!(
        error.current_context(),
        PulsarTopicIngestorError::RemoveSubscription { .. }
    ));
    assert!(
        holds_pulsar_failure(&error, |failure| matches!(
            failure,
            PulsarTopicIngestorError::RemovalDeadline { budget: actual } if *actual == budget
        )),
        "the removal ends at its budget, got {error:?}"
    );
}

#[test]
fn only_a_refusal_for_a_durable_subscription_says_the_subscription_exists() {
    let exists = format!("{DURABLE_SUBSCRIPTION_EXISTS}.");
    let refusal = |error: PulsarServerError, reason: &str| {
        PulsarConnectionError::PulsarError(Some(error), Some(reason.to_string()))
    };
    let found = [
        PulsarError::Connection(refusal(PulsarServerError::NotAllowedError, &exists)),
        PulsarError::Consumer(PulsarConsumerError::Connection(refusal(
            PulsarServerError::NotAllowedError,
            &exists,
        ))),
    ];
    for error in &found {
        assert!(
            PulsarTopicIngestorPlan::refused_for_a_durable_subscription(error),
            "{error}"
        );
    }
    let not_found = [
        PulsarError::Connection(refusal(
            PulsarServerError::NotAllowedError,
            "Exceed the maximum number of subscriptions of the topic: orders",
        )),
        PulsarError::Connection(refusal(PulsarServerError::ServiceNotReady, &exists)),
        PulsarError::Connection(PulsarConnectionError::PulsarError(
            Some(PulsarServerError::NotAllowedError),
            None,
        )),
        PulsarError::Connection(PulsarConnectionError::Disconnected),
    ];
    for error in &not_found {
        assert!(
            !PulsarTopicIngestorPlan::refused_for_a_durable_subscription(error),
            "{error}"
        );
    }
}
