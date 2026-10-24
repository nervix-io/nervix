//! Tests of the production consumer batch owner.
//!
//! Layer: test harness.
//! - **Owns.** Batch size, timeout, cancellation and deferred failure assertions.
//! - **Depends on.** The production batch reader and transport delivery fixtures.
//! - **Must not know.** Graphs, placement, live brokers or acknowledgement trees.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::IngestorMessage as _;
use nervix_primitives::{
    stream::{iter, wrappers::ReceiverStream},
    sync::mpsc,
};

use super::*;

fn delivery(id: u8) -> Delivery {
    Delivery::mock(u64::from(id), "".into(), "orders".into(), false, vec![id])
}

fn request() -> IngestorBatchRequest {
    IngestorBatchRequest {
        max_messages: NonZeroUsize::new(4).assured("the test requests four messages"),
        batch_timeout: Some(Duration::from_millis(100)),
    }
}

fn ids(batch: IngestorBatch<RabbitMqQueueIngestorMessage>) -> Vec<u8> {
    let messages = match batch {
        IngestorBatch::Messages(messages) | IngestorBatch::Interrupted { gathered: messages } => {
            messages
        }
        _ => panic!("a consumer batch either returns its deliveries or is interrupted"),
    };
    messages
        .iter()
        .map(|message| message.payload()[0])
        .collect()
}

#[nervix_primitives::test]
async fn a_full_batch_leaves_every_delivery_beyond_its_requested_maximum_for_the_next_read() {
    let stream = iter((1..=5).map(|id| Ok(delivery(id))));
    let mut reader = RabbitMqBatchReader::new(stream);
    let cancellation = IngestorReadCancellation::new();
    let first = reader
        .read(request(), &cancellation)
        .await
        .assured("the stream has a full batch");
    assert_eq!(ids(first), [1, 2, 3, 4]);
    let next = reader
        .read(request(), &cancellation)
        .await
        .assured("the partial batch precedes the stream closure");
    assert_eq!(ids(next), [5]);
}

#[nervix_primitives::test]
async fn a_closed_stream_hands_over_every_gathered_delivery_before_its_failure() {
    let mut reader = RabbitMqBatchReader::new(iter([Ok(delivery(1)), Ok(delivery(2))]));
    let cancellation = IngestorReadCancellation::new();
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("gathered deliveries are returned");
    assert_eq!(ids(batch), [1, 2]);
    let failure = reader
        .read(request(), &cancellation)
        .await
        .err()
        .assured("closure is deferred");
    assert!(matches!(
        failure.current_context(),
        RabbitMqQueueIngestorError::ConsumerClosed
    ));
}

#[nervix_primitives::test]
async fn a_transport_failure_keeps_its_typed_cause_after_handing_over_the_batch() {
    let cause: Error =
        lapin::ErrorKind::InvalidConnectionState(lapin::ConnectionState::Closed).into();
    let mut reader = RabbitMqBatchReader::new(iter([Ok(delivery(7)), Err(cause)]));
    let cancellation = IngestorReadCancellation::new();
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("the gathered delivery is returned");
    assert_eq!(ids(batch), [7]);
    let failure = reader
        .read(request(), &cancellation)
        .await
        .err()
        .assured("the failure is deferred");
    assert!(matches!(
        failure.current_context(),
        RabbitMqQueueIngestorError::Receive
    ));
    let cause = failure
        .downcast_ref::<Error>()
        .assured("the original driver cause is retained");
    assert!(matches!(
        cause.kind(),
        lapin::ErrorKind::InvalidConnectionState(lapin::ConnectionState::Closed)
    ));
}

#[nervix_primitives::test]
async fn cancellation_hands_over_the_delivery_that_was_already_ready() {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(Ok(delivery(3)))
        .await
        .assured("the receiver is held");
    let mut reader = RabbitMqBatchReader::new(ReceiverStream::new(receiver));
    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("cancellation is an ordinary outcome");
    assert!(matches!(&batch, IngestorBatch::Interrupted { .. }));
    assert_eq!(ids(batch), [3]);
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("an empty interrupted read succeeds");
    assert!(ids(batch).is_empty());
}

#[nervix_primitives::test(start_paused = true)]
async fn the_batch_deadline_starts_after_the_first_delivery_and_leaves_the_next_one_intact() {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(Ok(delivery(1)))
        .await
        .assured("the receiver is held");
    let mut reader = RabbitMqBatchReader::new(ReceiverStream::new(receiver));
    let cancellation = IngestorReadCancellation::new();
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("the deadline returns a partial batch");
    assert_eq!(ids(batch), [1]);
    sender
        .send(Ok(delivery(2)))
        .await
        .assured("the next read still owns its stream");
    let batch = reader
        .read(request(), &cancellation)
        .await
        .assured("the next deadline returns its own batch");
    assert_eq!(ids(batch), [2]);
}

#[nervix_primitives::test]
async fn an_unrepresentable_batch_deadline_does_not_discard_the_first_delivery() {
    let mut reader = RabbitMqBatchReader::new(iter([Ok(delivery(9))]));
    let cancellation = IngestorReadCancellation::new();
    let request = IngestorBatchRequest {
        batch_timeout: Some(Duration::MAX),
        ..request()
    };
    let batch = reader
        .read(request, &cancellation)
        .await
        .assured("a deadline refusal retains the payload");
    assert_eq!(ids(batch), [9]);
    let failure = reader
        .read(request, &cancellation)
        .await
        .err()
        .assured("the deadline failure is reported next");
    assert!(matches!(
        failure.current_context(),
        RabbitMqQueueIngestorError::BatchDeadline
    ));
}
