//! The Kafka ingestor's reads that the host cancels, against librdkafka's in-process mock cluster.
//!
//! A read the host has already cancelled takes only the records its consumer holds and then ends,
//! so a test repeats such reads until the consumer has fetched what the broker stores.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestorBatch, IngestorBatchRequest, IngestorMessage,
    IngestorReadCancellation,
};
use nervix_primitives::time::{Instant, sleep};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

use super::{
    KafkaGroupCommit,
    commit_tests::{READ_DEADLINE, cluster, publish, read_positions, subscribed_source},
};

/// How long a test lets the consumer fetch before it reads again.
const FETCH_INTERVAL: Duration = Duration::from_millis(10);

#[nervix_primitives::test]
async fn an_invalid_timestamp_returns_the_gathered_prefix_before_its_typed_failure() {
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
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(8).assured("eight is a nonzero batch bound"),
        batch_timeout: Some(Duration::from_secs(1)),
    };
    let cancellation = IngestorReadCancellation::new();
    let prefix =
        nervix_primitives::time::timeout(READ_DEADLINE, source.next_batch(request, &cancellation))
            .await
            .assured("the group forms within the test observation deadline")
            .assured("a later refused record does not discard the gathered prefix");
    let IngestorBatch::Messages(prefix) = prefix else {
        panic!("an uncancelled read returns its gathered messages");
    };
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].position().offset, 1);

    let cancelled = IngestorReadCancellation::new();
    cancelled.cancel();
    let stopped =
        nervix_primitives::time::timeout(READ_DEADLINE, source.next_batch(request, &cancelled))
            .await
            .assured("a cancelled read ends within the test observation deadline")
            .assured("cancellation ends intake");
    assert!(matches!(stopped, IngestorBatch::Interrupted { gathered } if gathered.is_empty()));

    let next =
        nervix_primitives::time::timeout(READ_DEADLINE, source.next_batch(request, &cancellation))
            .await
            .assured("the retained failure is reported within the test observation deadline");
    let Err(error) = next else {
        panic!("the next admitted read reports the retained timestamp refusal");
    };
    assert!(error.contains::<nervix_models::TimestampError>());
    let suffix = read_positions(&mut source).await;
    assert_eq!(suffix.len(), 1);
    assert_eq!(suffix[0].offset, 3);
}

#[nervix_primitives::test]
async fn a_cancelled_parallel_read_returns_every_record_it_gathered() {
    let cluster = cluster();
    for _record in 0..4 {
        publish(&cluster).await;
    }
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    // The first read waits until the group has formed and the consumer has fetched. A position
    // names the next unread offset, so the records at offsets 0 to 3 have positions 1 to 4.
    let first = read_positions(&mut source).await;
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
        let batch = source
            .next_batch(request, &cancellation)
            .await
            .assured("the source reads the records its broker serves");
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
