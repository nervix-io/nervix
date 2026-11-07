//! The Kafka ingestor's reads that the host cancels, against librdkafka's in-process mock cluster.
//!
//! A read the host has already cancelled takes only the records its consumer holds and then ends,
//! so a test repeats such reads until the consumer has fetched what the broker stores.

use std::{num::NonZeroUsize, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestorBatch, IngestorBatchRequest, IngestorMessage,
    IngestorReadCancellation, RetainedIngestMetadata, RetainedIngestMetadataValue as Value,
};
use nervix_models::{IngestorMetadataType, Timestamp};
use nervix_primitives::time::{Instant, sleep};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

use super::{
    KafkaGroupCommit, KafkaTopicIngestorError,
    commit_tests::{READ_DEADLINE, cluster, publish, read_positions, subscribed_ingestor},
};

/// How long a test lets the consumer fetch before it reads again.
const FETCH_INTERVAL: Duration = Duration::from_millis(10);

#[nervix_primitives::test]
async fn a_record_whose_timestamp_no_datetime_holds_is_read_between_its_neighbours() {
    let cluster = cluster();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()
        .assured("the producer connects to the in-process broker");
    for millis in [1_234, i64::MAX, 5_678] {
        producer
            .send(
                FutureRecord::<(), str>::to("orders")
                    .partition(0)
                    .payload("order")
                    .timestamp(millis),
                READ_DEADLINE,
            )
            .await
            .map_err(|(error, _)| error)
            .assured("the mock broker stores every current Kafka record");
    }
    let mut ingestor = subscribed_ingestor(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(8).assured("eight is a nonzero batch bound"),
        batch_timeout: Some(Duration::from_secs(1)),
    };
    let cancellation = IngestorReadCancellation::new();
    let mut messages = Vec::new();
    while messages.len() < 3 {
        let batch = nervix_primitives::time::timeout(
            READ_DEADLINE,
            ingestor.next_batch(request, &cancellation),
        )
        .await
        .assured("the group forms within the test observation deadline")
        .assured("a record whose timestamp no DATETIME holds does not fail the read");
        let IngestorBatch::Messages(batch) = batch else {
            panic!("an uncancelled read returns its gathered messages");
        };
        messages.extend(batch);
    }

    let mut readings = Vec::with_capacity(messages.len());
    for message in &messages {
        let retained = RetainedIngestMetadata::capture(message.metadata().fields);
        let [_topic, _partition, offset, _key, time, kind, _tombstone] = retained.values() else {
            panic!("a KAFKA TOPIC record hands over the seven fields its entity declares");
        };
        readings.push([offset.clone(), time.clone(), kind.clone()]);
    }
    let create_time = Value::String("create_time".to_string());
    assert_eq!(
        readings,
        [
            [
                Value::I64(0),
                Value::Datetime(Timestamp::from_unix_nanos(1_234_000_000)),
                create_time.clone(),
            ],
            [
                Value::I64(1),
                Value::Unrepresentable {
                    ty: IngestorMetadataType::Datetime,
                    value: "the Kafka timestamp 9223372036854775807 milliseconds after the Unix \
                            epoch"
                        .to_string(),
                },
                create_time.clone(),
            ],
            [
                Value::I64(2),
                Value::Datetime(Timestamp::from_unix_nanos(5_678_000_000)),
                create_time,
            ],
        ],
        "every record is read in order, its timestamp handed over as its kind and value"
    );
}

#[nervix_primitives::test]
async fn a_failure_retained_after_a_gathered_prefix_is_reported_by_the_next_admitted_read() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut ingestor = subscribed_ingestor(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    // What a collected read retains when a record after its gathered prefix has no position to
    // acknowledge, which only a record at the last offset Kafka's signed offsets reach has.
    ingestor.pending_read_error = Some(Report::new(KafkaTopicIngestorError::OffsetOverflow {
        topic: "orders".to_string(),
        partition: 0,
    }));
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(8).assured("eight is a nonzero batch bound"),
        batch_timeout: Some(Duration::from_secs(1)),
    };

    let cancelled = IngestorReadCancellation::new();
    cancelled.cancel();
    let stopped =
        nervix_primitives::time::timeout(READ_DEADLINE, ingestor.next_batch(request, &cancelled))
            .await
            .assured("a cancelled read ends within the test observation deadline")
            .assured("cancellation ends intake");
    assert!(matches!(stopped, IngestorBatch::Interrupted { gathered } if gathered.is_empty()));

    let cancellation = IngestorReadCancellation::new();
    let next = nervix_primitives::time::timeout(
        READ_DEADLINE,
        ingestor.next_batch(request, &cancellation),
    )
    .await
    .assured("the retained failure is reported within the test observation deadline");
    let Err(error) = next else {
        panic!("the next admitted read reports the retained failure");
    };
    assert!(matches!(
        error.downcast_ref::<KafkaTopicIngestorError>(),
        Some(KafkaTopicIngestorError::OffsetOverflow { partition: 0, .. })
    ));
    let positions = read_positions(&mut ingestor).await;
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].offset, 1);
}

#[nervix_primitives::test]
async fn a_cancelled_parallel_read_returns_every_record_it_gathered() {
    let cluster = cluster();
    for _record in 0..4 {
        publish(&cluster).await;
    }
    let mut ingestor = subscribed_ingestor(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    // The first read waits until the group has formed and the consumer has fetched. A position
    // names the next unread offset, so the records at offsets 0 to 3 have positions 1 to 4.
    let first = read_positions(&mut ingestor).await;
    let mut next_offsets = Vec::new();
    for position in &first {
        next_offsets.push(position.offset);
    }

    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(8).assured("eight is a nonzero batch bound"),
        batch_timeout: Some(Duration::from_secs(3600)),
    };
    let deadline = Instant::now()
        .checked_add(READ_DEADLINE)
        .assured("a minute after now fits the monotonic clock");
    while next_offsets.len() < 4 {
        let batch = ingestor
            .next_batch(request, &cancellation)
            .await
            .assured("the ingestor reads the records its broker serves");
        let IngestorBatch::Interrupted { gathered } = batch else {
            panic!("a read the host cancelled ends as interrupted");
        };
        if gathered.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the consumer fetched the stored records within the read deadline"
            );
            sleep(FETCH_INTERVAL).await;
            continue;
        }
        for message in &gathered {
            next_offsets.push(message.position().offset);
        }
    }

    assert_eq!(
        next_offsets,
        [1, 2, 3, 4],
        "the interrupted reads returned every record they took, each exactly once"
    );
}
