//! How a read meets the host's cancellation, observed against a loopback listener that stands in
//! for the SQS service and answers the one receive it takes when the check lets it, and against a
//! queue that never answers on a paused clock.
//!
//! The stand-in stops listening once it has taken that receive, so a read that sent another one
//! would fail to connect instead of returning a batch.

use std::{future::Future, net::Ipv4Addr, pin::pin, time::Duration};

use aws_sdk_sqs::types::Message as SqsMessage;
use nervix_connector::{
    BrokerIngestorConnector as _, IngestorBatch, IngestorError, IngestorMessage as _,
    IngestorReadCancellation,
};
use nervix_primitives::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio::io::AsyncWriteExt as _;

use super::{
    CANCELLED_RECEIVE_BUDGET, LONG_POLL, QueueReceive, ReceiveRequest, SQS, SqsQueueIngestor,
    SqsQueueIngestorError, SqsQueueIngestorMessage, SqsQueueIngestorPosition,
    SqsQueueIngestorResult,
};
use crate::queue::connection_tests::{
    Fixture, GENEROUS_BUDGET, QUEUE, RefusedEndpoint, batch_request, client_config, open_source,
    read_request, silent_service,
};

/// The body of the one message a stand-in answers a receive with.
const BODY: &str = r#"{"user_id":42}"#;
/// The receipt handle that deletes that message.
const RECEIPT_HANDLE: &str = "receipt-1";

/// What a stand-in service answers the receive it holds with.
#[derive(Clone, Copy, Debug)]
enum ReceiveAnswer {
    /// The one message the long poll brought.
    Message,
    /// No message: the long poll ended empty.
    Empty,
}

impl ReceiveAnswer {
    /// The whole HTTP response, after which the stand-in closes the connection.
    fn response(self) -> String {
        let body = match self {
            Self::Message => format!(
                r#"{{"Messages":[{{"MessageId":"message-1","ReceiptHandle":"{RECEIPT_HANDLE}","Body":"{}"}}]}}"#,
                BODY.replace('"', "\\\"")
            ),
            Self::Empty => "{}".to_string(),
        };
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }
}

/// A service on `listener` that takes one receive, stops listening, reports that the receive
/// arrived, and answers it with `answer` once the check lets it. Returns the request it took.
fn held_receive(
    listener: TcpListener,
    answer: ReceiveAnswer,
    arrived: oneshot::Sender<()>,
    answer_now: oneshot::Receiver<()>,
) -> JoinHandle<String> {
    nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("the read under test dials this listener");
        drop(listener);
        let request = read_request(&mut stream).await;
        arrived
            .send(())
            .expect("the check waits for the receive to arrive");
        answer_now
            .await
            .expect("the check lets the service answer once it cancelled the read");
        stream
            .write_all(answer.response().as_bytes())
            .await
            .expect("the read under test still waits for the receive's answer");
        request
    })
}

/// A ingestor instance polling the queue of the stand-in on `listener`, whose URL it already looked
/// up.
async fn polling_source(fixture: &Fixture, listener: &TcpListener) -> SqsQueueIngestor {
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await;
    source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));
    source
}

/// What the host does once the read's receive reached the stand-in, before the stand-in answers it.
#[derive(Clone, Copy, Debug)]
enum HostAction {
    /// The host keeps waiting for the read.
    Waits,
    /// The host cancels the read.
    Cancels,
}

/// What a read returned while the stand-in held its receive, and the stand-in that held it.
struct HeldRead {
    batch: IngestorBatch<SqsQueueIngestorMessage>,
    /// Returns the receive the stand-in took.
    service: JoinHandle<String>,
}

/// Starts a read of an ingestor polling a stand-in that answers its receive with `answer`, lets the
/// host act once the receive reached the stand-in, and only then lets the stand-in answer.
async fn read_while_the_receive_is_held(
    fixture: &Fixture,
    answer: ReceiveAnswer,
    action: HostAction,
) -> HeldRead {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let mut source = polling_source(fixture, &listener).await;
    let (arrived, receive_arrived) = oneshot::channel();
    let (answer_now, answer_held) = oneshot::channel();
    let service = held_receive(listener, answer, arrived, answer_held);

    let cancellation = IngestorReadCancellation::new();
    let read = source.next_batch(batch_request(), &cancellation);
    let host = async {
        receive_arrived
            .await
            .expect("the read sends its receive to the stand-in");
        if let HostAction::Cancels = action {
            cancellation.cancel();
            // The read observes the cancellation while its receive still waits for the answer.
            nervix_primitives::task::yield_now().await;
        }
        answer_now
            .send(())
            .expect("the stand-in waits to answer the receive it holds");
    };
    let (batch, ()) = tokio::join!(read, host);

    HeldRead {
        batch: batch.expect("a receive the service answers fails no read"),
        service,
    }
}

#[nervix_primitives::test]
async fn a_read_hands_over_the_message_its_receive_brings() {
    let fixture = Fixture::start().await;

    let read =
        read_while_the_receive_is_held(&fixture, ReceiveAnswer::Message, HostAction::Waits).await;

    let IngestorBatch::Messages(messages) = read.batch else {
        panic!("a receive that brings a message completes the read with it");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload(), BODY.as_bytes());
    read.service.await.expect("the stand-in does not panic");
}

#[nervix_primitives::test]
async fn a_receive_in_flight_when_the_host_cancels_the_read_still_hands_over_its_message() {
    let fixture = Fixture::start().await;

    let read =
        read_while_the_receive_is_held(&fixture, ReceiveAnswer::Message, HostAction::Cancels).await;

    // The cancelled read ends interrupted, with every message it had already taken.
    let IngestorBatch::Interrupted { gathered: messages } = read.batch else {
        panic!("a read the host cancelled ends interrupted");
    };
    assert_eq!(
        messages.len(),
        1,
        "the read abandoned the receive it had in flight, leaving its message invisible until its \
         visibility timeout"
    );
    assert_eq!(messages[0].payload(), BODY.as_bytes());
    assert_eq!(
        messages[0].position().receipt_handle,
        RECEIPT_HANDLE,
        "the message the read hands over can be deleted once it is acknowledged"
    );
    let request = read.service.await.expect("the stand-in does not panic");
    let head = request.to_ascii_lowercase();
    assert!(
        head.contains("x-amz-target: amazonsqs.receivemessage"),
        "{request}"
    );
    assert!(request.contains(r#""MaxNumberOfMessages":1"#), "{request}");
    assert!(request.contains(r#""WaitTimeSeconds":1"#), "{request}");
}

#[nervix_primitives::test]
async fn a_cancelled_read_whose_receive_brings_nothing_sends_no_further_receive() {
    let fixture = Fixture::start().await;

    let read =
        read_while_the_receive_is_held(&fixture, ReceiveAnswer::Empty, HostAction::Cancels).await;

    let IngestorBatch::Interrupted { gathered } = read.batch else {
        panic!("a cancelled read that took nothing ends interrupted");
    };
    assert!(gathered.is_empty());
    read.service.await.expect("the stand-in does not panic");
}

/// A queue whose service takes every receive and never answers it.
struct SilentQueue;

impl QueueReceive for SilentQueue {
    fn receive(
        &self,
        _receive: ReceiveRequest,
    ) -> impl Future<Output = SqsQueueIngestorResult<Vec<SqsMessage>>> + Send {
        std::future::pending()
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_cancelled_read_waits_for_a_receive_the_service_does_not_answer_only_until_its_budget() {
    let cancellation = IngestorReadCancellation::new();
    let mut read = pin!(SqsQueueIngestor::read_batch(
        &SilentQueue,
        batch_request(),
        &cancellation
    ));

    // A read nobody cancelled has no budget: it waits for its receive however long that takes.
    let uncancelled = timeout(Duration::from_secs(3_600), &mut read).await;
    assert!(
        uncancelled.is_err(),
        "a read nobody cancelled ended while its receive waited"
    );
    let cancelled_at = Instant::now();
    cancellation.cancel();
    let Err(report) = read.await else {
        panic!("a receive the service never answers cannot complete the cancelled read");
    };

    assert_eq!(
        Instant::now().duration_since(cancelled_at),
        CANCELLED_RECEIVE_BUDGET
    );
    assert_eq!(
        report.current_context(),
        &IngestorError::Read { connector: SQS }
    );
    assert!(
        matches!(
            report.downcast_ref::<SqsQueueIngestorError>(),
            Some(SqsQueueIngestorError::CancelledReceiveUnanswered { budget })
                if *budget == CANCELLED_RECEIVE_BUDGET
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn a_receive_that_cannot_reach_the_service_fails_the_read() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await;
    source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));

    let Err(report) = source
        .next_batch(batch_request(), &IngestorReadCancellation::new())
        .await
    else {
        panic!("a refused connection cannot return a batch");
    };

    assert_eq!(
        report.current_context(),
        &IngestorError::Read { connector: SQS }
    );
    assert!(
        matches!(
            report.downcast_ref::<SqsQueueIngestorError>(),
            Some(SqsQueueIngestorError::Receive)
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn a_receive_the_service_never_answers_ends_within_timeout_ms_after_its_long_poll() {
    let fixture = Fixture::start().await;
    let budget = Duration::from_millis(300);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let mut config = client_config(&format!("http://127.0.0.1:{port}"));
    config.push(nervix_models::ClientConfigEntry {
        key: "timeout_ms".to_string(),
        value: budget.as_millis().to_string(),
    });
    let mut source = open_source(&config, fixture.dns()).await;
    source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));
    // The service takes the receive and never answers it, holding its connection open.
    let service = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("the read under test dials this listener");
        let request = read_request(&mut stream).await;
        (request, stream)
    });

    let started = Instant::now();
    let read = timeout(
        GENEROUS_BUDGET,
        source.next_batch(batch_request(), &IngestorReadCancellation::new()),
    )
    .await
    .expect("a receive the service never answers ends within its long poll and timeout_ms");
    let waited = started.elapsed();

    let Err(report) = read else {
        panic!("a receive the service never answers brings no message");
    };
    assert_eq!(
        report.current_context(),
        &IngestorError::Read { connector: SQS }
    );
    assert_eq!(
        report
            .downcast_ref::<nervix_connector::DeadlineElapsed>()
            .map(|elapsed| (elapsed.operation, elapsed.key, elapsed.bound)),
        Some((
            "an SQS receive after its long poll",
            nervix_models::DeadlineKey::Timeout,
            budget
        )),
        "{report:?}"
    );
    // The deadline counts from the end of the long poll the receive asked the service to hold it
    // for, so the read waited at least that long poll and its timeout_ms.
    let bound = LONG_POLL
        .checked_add(budget)
        .expect("a one-second long poll and a 300 ms deadline fit a duration");
    assert!(
        waited >= bound,
        "the receive ended after {waited:?}, before its long poll and timeout_ms"
    );
    let (request, _held) = service.await.expect("the stand-in does not panic");
    assert!(
        request
            .to_ascii_lowercase()
            .contains("x-amz-target: amazonsqs.receivemessage"),
        "{request}"
    );
}

/// A source whose client's `timeout_ms` is its `budget`, addressing a queue on a service that
/// accepts every connection and never answers, with the service and the connections it holds open.
struct UnansweredSource {
    source: SqsQueueIngestor,
    budget: Duration,
    _service: JoinHandle<()>,
    _connections: nervix_primitives::sync::mpsc::Receiver<nervix_primitives::net::TcpStream>,
}

impl UnansweredSource {
    async fn start(fixture: &Fixture) -> Self {
        let budget = Duration::from_millis(300);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback port is available");
        let port = listener
            .local_addr()
            .expect("a bound listener has an address")
            .port();
        let (accepted, connections) = nervix_primitives::sync::mpsc::channel(8);
        let service = silent_service(listener, accepted);
        let mut config = client_config(&format!("http://127.0.0.1:{port}"));
        config.push(nervix_models::ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: budget.as_millis().to_string(),
        });
        let mut source = open_source(&config, fixture.dns()).await;
        source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));
        Self {
            source,
            budget,
            _service: service,
            _connections: connections,
        }
    }

    /// The position of a message the source received, which a settlement addresses.
    fn position() -> SqsQueueIngestorPosition {
        SqsQueueIngestorPosition {
            receipt_handle: "receipt-handle".to_string(),
        }
    }
}

#[nervix_primitives::test]
async fn a_batch_delete_the_service_never_answers_ends_within_timeout_ms() {
    let fixture = Fixture::start().await;
    let mut unanswered = UnansweredSource::start(&fixture).await;

    let acknowledged = timeout(
        GENEROUS_BUDGET,
        unanswered
            .source
            .acknowledge(&[UnansweredSource::position()]),
    )
    .await
    .expect("a batch delete the service never answers ends within the client's timeout_ms");

    let Err(report) = acknowledged else {
        panic!("a batch delete the service never answers acknowledges nothing");
    };
    assert_eq!(
        report.current_context(),
        &IngestorError::Acknowledge { connector: SQS }
    );
    assert_eq!(
        report
            .downcast_ref::<nervix_connector::DeadlineElapsed>()
            .map(|elapsed| (elapsed.operation, elapsed.key, elapsed.bound)),
        Some((
            "an SQS batch delete",
            nervix_models::DeadlineKey::Timeout,
            unanswered.budget
        )),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn a_visibility_change_the_service_never_answers_ends_within_timeout_ms() {
    let fixture = Fixture::start().await;
    let mut unanswered = UnansweredSource::start(&fixture).await;

    let rejected = timeout(
        GENEROUS_BUDGET,
        unanswered.source.reject(&[UnansweredSource::position()]),
    )
    .await
    .expect("a visibility change the service never answers ends within the client's timeout_ms");

    let Err(report) = rejected else {
        panic!("a visibility change the service never answers makes nothing visible again");
    };
    assert_eq!(
        report.current_context(),
        &IngestorError::Reject { connector: SQS }
    );
    assert_eq!(
        report
            .downcast_ref::<nervix_connector::DeadlineElapsed>()
            .map(|elapsed| (elapsed.operation, elapsed.key, elapsed.bound)),
        Some((
            "an SQS visibility change",
            nervix_models::DeadlineKey::Timeout,
            unanswered.budget
        )),
        "{report:?}"
    );
}
