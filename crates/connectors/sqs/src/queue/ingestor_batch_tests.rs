//! How an SQS read gathers a batch, and how acknowledging and rejecting settle one.
//!
//! A batch window is observed against a scripted queue on a paused clock, whose long poll lets the
//! clock run on as the service's own wait would. The requests a source sends are observed against
//! a loopback stand-in for the SQS service that answers each request it takes with the next answer
//! of a script, closes its connection, and stops listening after its last answer, so a source that
//! sent one request more would fail to connect.

use std::{collections::VecDeque, net::Ipv4Addr, num::NonZeroUsize, time::Duration};

use aws_sdk_sqs::types::Message as SqsMessage;
use error_stack::Report;
use nervix_connector::{
    BrokerIngestorConnector as _, IngestorBatch, IngestorBatchRequest, IngestorError,
    IngestorMessage as _, IngestorReadCancellation,
};
use nervix_primitives::{net::TcpListener, sync::Mutex, task::JoinHandle, time::advance};
use tokio::io::AsyncWriteExt as _;

use super::{
    LONG_POLL, QueueReceive, ReceiveRequest, ReceiveWait, SQS, SqsQueueIngestor,
    SqsQueueIngestorError, SqsQueueIngestorMessage, SqsQueueIngestorPosition,
    SqsQueueIngestorResult,
};
use crate::queue::connection_tests::{
    Fixture, QUEUE, RefusedEndpoint, client_config, open_source, read_request,
};

/// A message whose body is its id and whose receipt handle names it.
fn message(id: u64) -> SqsMessage {
    SqsMessage::builder()
        .message_id(format!("message-{id}"))
        .body(id.to_string())
        .receipt_handle(format!("receipt-{id}"))
        .build()
}

/// A message the service gave no receipt handle.
fn message_without_receipt_handle(id: u64) -> SqsMessage {
    SqsMessage::builder()
        .message_id(format!("message-{id}"))
        .body(id.to_string())
        .build()
}

/// The ids of the messages `messages` hands the host.
fn ids(messages: &[SqsQueueIngestorMessage]) -> Vec<u64> {
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        let body = std::str::from_utf8(message.payload()).expect("a check's bodies are ids");
        ids.push(body.parse().expect("a check's bodies are ids"));
    }
    ids
}

/// A host request for a batch of `max` messages within `window`.
fn request(max: usize, window: Option<Duration>) -> IngestorBatchRequest {
    IngestorBatchRequest {
        max_messages: NonZeroUsize::new(max).expect("a check asks for at least one message"),
        batch_timeout: window,
    }
}

/// A queue that answers each receive with the next answer of its script, then fails the next receive
/// where the script ends in a failure, and answers with nothing after that; it keeps every receive
/// it was sent. A long poll that brings nothing lets the paused clock run on by the long poll, as
/// the service's own wait would.
struct ScriptedQueue {
    script: Mutex<VecDeque<Vec<SqsMessage>>>,
    failure: Mutex<Option<Report<SqsQueueIngestorError>>>,
    receives: Mutex<Vec<ReceiveRequest>>,
}

impl ScriptedQueue {
    fn new(script: Vec<Vec<SqsMessage>>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            failure: Mutex::new(None),
            receives: Mutex::new(Vec::new()),
        }
    }

    /// A queue that answers `script` and fails the receive after its last answer with `failure`.
    fn failing_after(script: Vec<Vec<SqsMessage>>, failure: SqsQueueIngestorError) -> Self {
        Self {
            script: Mutex::new(script.into()),
            failure: Mutex::new(Some(Report::new(failure))),
            receives: Mutex::new(Vec::new()),
        }
    }

    async fn receives(&self) -> Vec<ReceiveRequest> {
        self.receives.lock().await.clone()
    }
}

impl QueueReceive for ScriptedQueue {
    async fn receive(&self, receive: ReceiveRequest) -> SqsQueueIngestorResult<Vec<SqsMessage>> {
        self.receives.lock().await.push(receive);
        let answer = self.script.lock().await.pop_front();
        if let Some(messages) = answer {
            return Ok(messages);
        }
        let failure = self.failure.lock().await.take();
        if let Some(failure) = failure {
            return Err(failure);
        }
        if receive.wait == ReceiveWait::LongPoll {
            advance(LONG_POLL).await;
        }
        Ok(Vec::new())
    }
}

fn long_poll(count: usize) -> ReceiveRequest {
    ReceiveRequest::new(
        NonZeroUsize::new(count).expect("a receive asks for a message"),
        ReceiveWait::LongPoll,
    )
}

fn immediate(count: usize) -> ReceiveRequest {
    ReceiveRequest::new(
        NonZeroUsize::new(count).expect("a receive asks for a message"),
        ReceiveWait::Immediate,
    )
}

/// What a read of `queue` hands the host for `request`, which must be a batch the host did not
/// interrupt, with the failure it leaves for the next read.
async fn read(
    queue: &ScriptedQueue,
    request: IngestorBatchRequest,
) -> (Vec<u64>, Option<Report<SqsQueueIngestorError>>) {
    let gathered = SqsQueueIngestor::read_batch(queue, request, &IngestorReadCancellation::new())
        .await
        .expect("a scripted queue fails no receive");
    let IngestorBatch::Messages(messages) = gathered.batch else {
        panic!("a read nobody cancelled hands over a batch");
    };
    (ids(&messages), gathered.deferred)
}

#[nervix_primitives::test(start_paused = true)]
async fn a_sequential_read_asks_one_receive_for_one_message() {
    let queue = ScriptedQueue::new(vec![vec![message(1)]]);

    let (handed, deferred) = read(&queue, request(1, None)).await;

    assert_eq!(handed, vec![1]);
    assert!(deferred.is_none());
    assert_eq!(queue.receives().await, vec![long_poll(1)]);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_parallel_read_asks_each_receive_for_what_its_batch_still_needs() {
    let queue = ScriptedQueue::new(vec![
        vec![message(1), message(2)],
        vec![message(3)],
        vec![message(4), message(5)],
    ]);

    let (handed, deferred) = read(&queue, request(5, Some(Duration::from_secs(3_600)))).await;

    assert_eq!(handed, vec![1, 2, 3, 4, 5]);
    assert!(deferred.is_none());
    assert_eq!(
        queue.receives().await,
        vec![long_poll(5), long_poll(3), long_poll(2)]
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_receive_asks_for_no_more_than_the_ten_messages_one_receive_returns() {
    let queue = ScriptedQueue::new(vec![vec![message(1)]]);

    let (handed, _) = read(&queue, request(64, None)).await;

    assert_eq!(handed, vec![1]);
    assert_eq!(queue.receives().await, vec![long_poll(10)]);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_window_without_room_for_a_long_poll_tops_up_once_without_waiting() {
    let queue = ScriptedQueue::new(vec![vec![message(1)]]);

    let (handed, _) = read(&queue, request(3, Some(Duration::from_millis(500)))).await;

    assert_eq!(handed, vec![1]);
    assert_eq!(queue.receives().await, vec![long_poll(3), immediate(2)]);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_window_ends_once_its_long_polls_used_it_up() {
    let queue = ScriptedQueue::new(vec![vec![message(1)]]);

    let (handed, _) = read(&queue, request(3, Some(Duration::from_millis(1_500)))).await;

    // The window opens with the first message: one long poll fits it, the half second it leaves
    // takes a receive that does not wait, and that one's empty answer ends the batch.
    assert_eq!(handed, vec![1]);
    assert_eq!(
        queue.receives().await,
        vec![long_poll(3), long_poll(2), immediate(2)]
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_window_that_has_passed_sends_no_further_receive() {
    let queue = ScriptedQueue::new(vec![vec![message(1)]]);

    let (handed, _) = read(&queue, request(3, Some(Duration::from_secs(1)))).await;

    assert_eq!(handed, vec![1]);
    assert_eq!(queue.receives().await, vec![long_poll(3), long_poll(2)]);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_window_longer_than_the_clock_holds_tops_up_until_the_batch_is_full() {
    let queue = ScriptedQueue::new(vec![vec![message(1)], vec![message(2)], vec![message(3)]]);

    let (handed, deferred) = read(&queue, request(3, Some(Duration::MAX))).await;

    assert_eq!(handed, vec![1, 2, 3]);
    assert!(deferred.is_none());
    assert_eq!(
        queue.receives().await,
        vec![long_poll(3), long_poll(2), long_poll(1)]
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn messages_gathered_before_a_failed_top_up_reach_the_host_before_its_failure() {
    let queue = ScriptedQueue::failing_after(
        vec![vec![message(1), message(2)]],
        SqsQueueIngestorError::Receive,
    );

    let (handed, deferred) = read(&queue, request(4, Some(Duration::from_secs(3_600)))).await;

    // The failure ends the gathering: the batch is not topped up past it.
    assert_eq!(handed, vec![1, 2]);
    let Some(deferred) = deferred else {
        panic!("the failed top-up leaves its failure for the next read");
    };
    assert_eq!(deferred.current_context(), &SqsQueueIngestorError::Receive);
    assert_eq!(queue.receives().await, vec![long_poll(4), long_poll(2)]);
}

#[test]
fn only_a_long_poll_asks_the_service_to_hold_a_receive() {
    assert_eq!(ReceiveWait::LongPoll.seconds(), 1);
    assert_eq!(ReceiveWait::Immediate.seconds(), 0);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_message_without_a_receipt_handle_is_a_typed_read_failure() {
    let queue = ScriptedQueue::new(vec![vec![message_without_receipt_handle(1)]]);

    let Err(report) =
        SqsQueueIngestor::read_batch(&queue, request(1, None), &IngestorReadCancellation::new())
            .await
    else {
        panic!("a message the source cannot settle fails the read");
    };

    assert_eq!(
        report.current_context(),
        &IngestorError::Read { connector: SQS }
    );
    assert_eq!(
        report.downcast_ref::<SqsQueueIngestorError>(),
        Some(&SqsQueueIngestorError::MissingReceiptHandle {
            message_id: Some("message-1".to_string()),
        })
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn messages_beside_one_without_a_receipt_handle_reach_the_host_before_its_failure() {
    let queue = ScriptedQueue::new(vec![
        vec![message(1), message_without_receipt_handle(2), message(3)],
        vec![message(4)],
    ]);

    let (handed, deferred) = read(&queue, request(4, Some(Duration::from_secs(3_600)))).await;

    // The failure ends the gathering: the batch is not topped up past it.
    assert_eq!(handed, vec![1, 3]);
    let Some(deferred) = deferred else {
        panic!("the message without a receipt handle leaves its failure for the next read");
    };
    assert_eq!(
        deferred.current_context(),
        &SqsQueueIngestorError::MissingReceiptHandle {
            message_id: Some("message-2".to_string()),
        }
    );
    assert_eq!(queue.receives().await, vec![long_poll(4)]);
}

#[nervix_primitives::test]
async fn a_failure_left_by_the_last_read_fails_the_next_one_before_it_receives() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await;
    source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));
    source.failure = Some(Report::new(SqsQueueIngestorError::MissingReceiptHandle {
        message_id: None,
    }));

    let Err(report) = source
        .next_batch(request(1, None), &IngestorReadCancellation::new())
        .await
    else {
        panic!("the left failure fails the read");
    };

    assert!(
        matches!(
            report.downcast_ref::<SqsQueueIngestorError>(),
            Some(SqsQueueIngestorError::MissingReceiptHandle { .. })
        ),
        "the read reported the left failure, not a refused receive: {report:?}"
    );
    assert!(source.failure.is_none(), "a failure is reported once");
}

/// A stand-in that answers one request per connection with the next body of `answers` and returns
/// the requests it took, then stops listening.
fn scripted_service(listener: TcpListener, answers: Vec<String>) -> JoinHandle<Vec<String>> {
    nervix_primitives::task::spawn(async move {
        let mut requests = Vec::with_capacity(answers.len());
        for body in answers {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("the source under test dials this listener");
            requests.push(read_request(&mut stream).await);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("the source under test waits for the answer");
        }
        requests
    })
}

/// An ingestor instance whose queue URL names the stand-in on `listener`.
async fn source_of(fixture: &Fixture, listener: &TcpListener) -> SqsQueueIngestor {
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

async fn loopback() -> TcpListener {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available")
}

fn positions(ids: std::ops::RangeInclusive<u64>) -> Vec<SqsQueueIngestorPosition> {
    let mut positions = Vec::new();
    for id in ids {
        positions.push(SqsQueueIngestorPosition {
            receipt_handle: format!("receipt-{id}"),
        });
    }
    positions
}

/// The answer to a batch request whose entries `0..entries` all succeeded.
fn all_succeeded(entries: usize) -> String {
    let mut successful = Vec::with_capacity(entries);
    for index in 0..entries {
        successful.push(format!(r#"{{"Id":"{index}"}}"#));
    }
    format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful.join(","))
}

/// The answer to a batch request whose entry `0` failed with `code`, by the sender's fault or the
/// service's, and whose other entries up to `entries` succeeded.
fn first_failed(entries: usize, code: &str, sender_fault: bool) -> String {
    let mut successful = Vec::with_capacity(entries);
    for index in 1..entries {
        successful.push(format!(r#"{{"Id":"{index}"}}"#));
    }
    format!(
        r#"{{"Successful":[{}],"Failed":[{{"Id":"0","SenderFault":{sender_fault},"Code":"{code}","Message":"refused by the check"}}]}}"#,
        successful.join(",")
    )
}

/// Whether `request` names the receipt handle of every message of `ids`.
fn names_every_receipt_handle(request: &str, ids: std::ops::RangeInclusive<u64>) -> bool {
    let mut named = true;
    for id in ids {
        named &= request.contains(&format!(r#""ReceiptHandle":"receipt-{id}""#));
    }
    named
}

#[nervix_primitives::test]
async fn a_read_asks_the_service_for_the_messages_its_batch_needs() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let answer =
        r#"{"Messages":[{"MessageId":"message-1","ReceiptHandle":"receipt-1","Body":"1"}]}"#;
    let service = scripted_service(listener, vec![answer.to_string()]);

    let batch = source
        .next_batch(request(7, None), &IngestorReadCancellation::new())
        .await
        .expect("a receive the service answers fails no read");

    let IngestorBatch::Messages(messages) = batch else {
        panic!("a receive that brings a message completes the read with it");
    };
    assert_eq!(ids(&messages), vec![1]);
    let requests = service.await.expect("the stand-in does not panic");
    let [receive] = requests.as_slice() else {
        panic!("a read without a batch window sends one receive: {requests:?}");
    };
    assert!(
        receive
            .to_ascii_lowercase()
            .contains("x-amz-target: amazonsqs.receivemessage"),
        "{receive}"
    );
    assert!(receive.contains(r#""MaxNumberOfMessages":7"#), "{receive}");
    assert!(receive.contains(r#""WaitTimeSeconds":1"#), "{receive}");
}

#[nervix_primitives::test]
async fn acknowledging_deletes_a_batch_with_one_request() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(listener, vec![all_succeeded(3)]);

    source
        .acknowledge(&positions(1..=3))
        .await
        .expect("the service deleted every message");

    let requests = service.await.expect("the stand-in does not panic");
    let [delete] = requests.as_slice() else {
        panic!("three messages take one batch deletion: {requests:?}");
    };
    assert!(
        delete
            .to_ascii_lowercase()
            .contains("x-amz-target: amazonsqs.deletemessagebatch"),
        "{delete}"
    );
    assert!(names_every_receipt_handle(delete, 1..=3), "{delete}");
}

#[nervix_primitives::test]
async fn acknowledging_more_than_ten_messages_sends_one_request_per_ten() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(listener, vec![all_succeeded(10), all_succeeded(2)]);

    source
        .acknowledge(&positions(1..=12))
        .await
        .expect("the service deleted every message");

    let requests = service.await.expect("the stand-in does not panic");
    let [first, second] = requests.as_slice() else {
        panic!("twelve messages take two batch deletions: {requests:?}");
    };
    assert!(names_every_receipt_handle(first, 1..=10), "{first}");
    assert!(names_every_receipt_handle(second, 11..=12), "{second}");
    assert!(!second.contains(r#""receipt-1""#), "{second}");
}

#[nervix_primitives::test]
async fn a_deletion_the_service_refuses_fails_the_acknowledgement() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(
        listener,
        vec![first_failed(3, "ReceiptHandleIsInvalid", true)],
    );

    let Err(report) = source.acknowledge(&positions(1..=3)).await else {
        panic!("a message the service did not delete is not acknowledged");
    };

    assert_eq!(
        report.current_context(),
        &IngestorError::Acknowledge { connector: SQS }
    );
    assert_eq!(
        report.downcast_ref::<SqsQueueIngestorError>(),
        Some(&SqsQueueIngestorError::DeleteRefused {
            refused: NonZeroUsize::MIN,
            requested: 3,
            code: "ReceiptHandleIsInvalid".to_string(),
        })
    );
    service.await.expect("the stand-in does not panic");
}

#[nervix_primitives::test]
async fn a_deletion_that_cannot_reach_the_service_fails_the_acknowledgement() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await;
    source.queue_url = Some(format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"));

    let Err(report) = source.acknowledge(&positions(1..=2)).await else {
        panic!("a refused connection deletes nothing");
    };

    assert_eq!(
        report.current_context(),
        &IngestorError::Acknowledge { connector: SQS }
    );
    assert!(
        matches!(
            report.downcast_ref::<SqsQueueIngestorError>(),
            Some(SqsQueueIngestorError::Delete)
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn rejecting_makes_every_message_visible_again_at_once() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(listener, vec![all_succeeded(3)]);

    source
        .reject(&positions(1..=3))
        .await
        .expect("the service made every message visible");

    let requests = service.await.expect("the stand-in does not panic");
    let [change] = requests.as_slice() else {
        panic!("three messages take one batch visibility change: {requests:?}");
    };
    assert!(
        change
            .to_ascii_lowercase()
            .contains("x-amz-target: amazonsqs.changemessagevisibilitybatch"),
        "{change}"
    );
    assert!(names_every_receipt_handle(change, 1..=3), "{change}");
    assert_eq!(
        change.matches(r#""VisibilityTimeout":0"#).count(),
        3,
        "every entry makes its message visible at once: {change}"
    );
}

#[nervix_primitives::test]
async fn a_rejected_message_no_longer_invisible_needs_nothing_more() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(listener, vec![first_failed(2, "MessageNotInflight", true)]);

    source
        .reject(&positions(1..=2))
        .await
        .expect("a message that is visible already needs no visibility change");

    service.await.expect("the stand-in does not panic");
}

#[nervix_primitives::test]
async fn a_visibility_change_the_service_fails_fails_the_rejection() {
    let fixture = Fixture::start().await;
    let listener = loopback().await;
    let mut source = source_of(&fixture, &listener).await;
    let service = scripted_service(listener, vec![first_failed(2, "InternalError", false)]);

    let Err(report) = source.reject(&positions(1..=2)).await else {
        panic!("a visibility change the service failed fails the rejection");
    };

    assert_eq!(
        report.current_context(),
        &IngestorError::Reject { connector: SQS }
    );
    assert_eq!(
        report.downcast_ref::<SqsQueueIngestorError>(),
        Some(&SqsQueueIngestorError::VisibilityFailed {
            failed: NonZeroUsize::MIN,
            requested: 2,
            code: "InternalError".to_string(),
        })
    );
    service.await.expect("the stand-in does not panic");
}

#[nervix_primitives::test]
async fn settling_no_messages_sends_no_request() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    // A source that never resumed has no queue URL, and an endpoint that refuses every connection
    // fails any request it would send.
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{}", endpoint.port())),
        fixture.dns(),
    )
    .await;

    source
        .acknowledge(&[])
        .await
        .expect("acknowledging no messages deletes nothing");
    source
        .reject(&[])
        .await
        .expect("rejecting no messages changes no visibility");
}
