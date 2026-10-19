//! The Pulsar source's reads that the host cancels, and its acknowledgements and redelivery
//! requests, against a broker on loopback.
//!
//! Each test drives the production source through the client's real consumer, which subscribes to
//! a broker that delivers the messages a test asks for and answers each acknowledgement as the test
//! chooses, so an acknowledgement counts as confirmed only once the broker answered it. The broker
//! serves only its first connection: a later one, such as a reconnection, is accepted and never
//! answered, as a broker the client cannot reach would leave it.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use nervix_connector::{
    BrokerSourceConnector, SourceBatch, SourceBatchRequest, SourceConnector, SourceError,
    SourceMessage, SourceReadCancellation,
};
use nervix_models::PulsarSubscriptionName;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{Instant, sleep, timeout},
};
use pulsar::{
    OperationRetryOptions, Pulsar, TokioExecutor,
    error::{ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError},
    message::{
        Codec as PulsarCodec, Message as PulsarFrame, Payload as PulsarPayload,
        proto::{
            BaseCommand, CommandAckResponse, CommandConnected, CommandLookupTopicResponse,
            CommandMessage, CommandPartitionedTopicMetadataResponse, CommandPong, CommandSuccess,
            MessageIdData, MessageMetadata, base_command::Type as CommandType,
            command_lookup_topic_response::LookupType as TopicLookupType,
            command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
        },
    },
};
use tokio_util::codec::Framed;

use super::{
    PulsarAckReceipts, PulsarRefusal, PulsarServerError, PulsarSource, PulsarSourceError,
    PulsarSourcePlan, PulsarSourcePosition, PulsarUnresolvedRequest,
};

const TOPIC: &str = "persistent://public/default/orders";

/// The client's operation timeout in these tests, which bounds every answer the consumer awaits.
const OPERATION_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a test waits for something the broker or the source reports.
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

/// A Pulsar broker on loopback that serves one shared-subscription consumer: it delivers
/// `messages` once the consumer asks for them, answers acknowledgements with `answer`, and reports
/// what it receives.
struct LoopbackBroker {
    service_url: String,
    received: mpsc::UnboundedReceiver<Received>,
}

impl LoopbackBroker {
    async fn start(messages: u64, answer: AckAnswer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        let addr = listener
            .local_addr()
            .expect("a bound listener has an address");
        let service_url = format!("pulsar://{addr}");
        let (received_tx, received) = mpsc::unbounded_channel();
        let lookup_url = service_url.clone();
        nervix_primitives::task::spawn(async move {
            let Ok((first, _)) = listener.accept().await else {
                return;
            };
            nervix_primitives::task::spawn(Self::serve(
                Framed::new(first, PulsarCodec),
                lookup_url,
                messages,
                answer,
                received_tx.clone(),
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
        }
    }

    async fn serve(
        mut connection: Framed<TcpStream, PulsarCodec>,
        service_url: String,
        messages: u64,
        answer: AckAnswer,
        received: mpsc::UnboundedSender<Received>,
    ) {
        let mut delivered = false;
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
                replies.push(Self::success(request.request_id));
            } else if let Some(flow) = command.flow {
                if !delivered {
                    delivered = true;
                    replies
                        .extend((0..messages).map(|entry| Self::message(flow.consumer_id, entry)));
                }
            } else if let Some(ack) = command.ack {
                let acknowledged = Received::Ack {
                    message_ids: ack.message_id,
                    carried_request_id: ack.request_id.is_some(),
                };
                if received.send(acknowledged).is_err() {
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
                if received.send(requested).is_err() {
                    return;
                }
            } else if let Some(request) = command.close_consumer {
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

    fn message(consumer_id: u64, entry_id: u64) -> PulsarFrame {
        PulsarFrame {
            command: BaseCommand {
                r#type: i32::from(CommandType::Message),
                message: Some(CommandMessage {
                    consumer_id,
                    message_id: message_id(entry_id),
                    redelivery_count: Some(0),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: Some(PulsarPayload {
                metadata: MessageMetadata {
                    producer_name: "loopback-producer".to_string(),
                    sequence_id: entry_id,
                    publish_time: 1,
                    ..Default::default()
                },
                data: format!("{{\"user_id\":{entry_id}}}").into_bytes(),
            }),
        }
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
}

/// The id of the loopback broker's message at `entry_id`.
fn message_id(entry_id: u64) -> MessageIdData {
    MessageIdData {
        ledger_id: 7,
        entry_id,
        ..Default::default()
    }
}

/// The source of one instance under an `ACK` mode, subscribed on `broker` through a client whose
/// operation timeout is short enough for a test to outlast.
async fn open(broker: &LoopbackBroker) -> PulsarSource {
    open_with(broker, PulsarAckReceipts::Awaited).await
}

async fn open_with(broker: &LoopbackBroker, ack_receipts: PulsarAckReceipts) -> PulsarSource {
    let client = Pulsar::builder(broker.service_url.clone(), TokioExecutor)
        .with_operation_retry_options(OperationRetryOptions {
            operation_timeout: OPERATION_TIMEOUT,
            ..Default::default()
        })
        .build()
        .await
        .expect("the loopback broker serves the client's handshake");
    let plan = PulsarSourcePlan {
        client,
        topic: TOPIC.to_string(),
        subscription: PulsarSubscriptionName::parse("orders-ingestor")
            .expect("the test subscription is a valid Pulsar subscription name"),
        consumer_name: "orders".to_string(),
        ack_receipts,
    };
    PulsarSource::open(&plan, 0)
        .await
        .expect("the loopback broker serves the subscription")
}

/// The positions of the first `count` messages the source reads.
async fn read(source: &mut PulsarSource, count: usize) -> Vec<PulsarSourcePosition> {
    let mut positions = Vec::new();
    while positions.len() < count {
        let request = SourceBatchRequest {
            max_messages: std::num::NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let batch = timeout(
            DEADLINE,
            source.next_batch(request, &SourceReadCancellation::new()),
        )
        .await
        .expect("the source reads the broker's next message")
        .expect("the loopback broker delivers its messages");
        let SourceBatch::Messages(messages) = batch else {
            panic!("an open source reads messages");
        };
        positions.extend(messages.iter().map(|message| message.position().clone()));
    }
    positions
}

/// How long a test lets the consumer receive before it reads again.
const RECEIVE_INTERVAL: Duration = Duration::from_millis(10);

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
    let cancellation = SourceReadCancellation::new();
    cancellation.cancel();
    let request = SourceBatchRequest {
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
        let SourceBatch::Interrupted { gathered } = batch else {
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

/// The Pulsar failure a source operation reports beneath its connector context.
fn pulsar_failure(error: &error_stack::Report<SourceError>) -> &PulsarSourceError {
    error
        .downcast_ref::<PulsarSourceError>()
        .expect("a Pulsar source failure carries its Pulsar cause")
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

    let PulsarSourceError::AcknowledgementRefused { refusal } = pulsar_failure(&error) else {
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
        SourceError::Acknowledge {
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
            PulsarSourceError::AcknowledgementUnconfirmed {
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
            PulsarSourceError::AcknowledgementUnconfirmed {
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
            PulsarSourceError::RedeliveryRequestUnsent {
                unresolved: PulsarUnresolvedRequest::OperationTimeout
            }
        ),
        "the reconnection holds the redelivery request past the operation timeout, got {error:?}"
    );
    assert!(matches!(
        error.current_context(),
        SourceError::Reject {
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

    let acknowledgement = PulsarSourceError::unsettled_acknowledgement(unclassified());
    assert!(matches!(
        acknowledgement.current_context(),
        PulsarSourceError::Acknowledge
    ));
    assert!(acknowledgement.contains::<PulsarConsumerError>());

    let redelivery = PulsarSourceError::unsent_redelivery_request(unclassified());
    assert!(matches!(
        redelivery.current_context(),
        PulsarSourceError::NegativelyAcknowledge
    ));
    assert!(redelivery.contains::<PulsarConsumerError>());

    // The broker answers no redelivery request, so a refusal can only come from an
    // acknowledgement.
    let refused = PulsarConsumerError::Connection(PulsarConnectionError::PulsarError(
        Some(PulsarServerError::ServiceNotReady),
        None,
    ));
    assert!(matches!(
        PulsarSourceError::unsent_redelivery_request(refused).current_context(),
        PulsarSourceError::NegativelyAcknowledge
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
        PulsarSourceError::AcknowledgementUnconfirmed {
            unresolved: PulsarUnresolvedRequest::OperationTimeout
        }
        .to_string(),
        "the Pulsar broker did not confirm an acknowledgement: the client's operation timeout \
         passed first"
    );
}
