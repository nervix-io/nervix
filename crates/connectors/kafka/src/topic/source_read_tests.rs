//! The Kafka source's reads that the host cancels, against librdkafka's in-process mock cluster.
//!
//! A read the host has already cancelled takes only the records its consumer holds and then ends,
//! so a test repeats such reads until the consumer has fetched what the broker stores.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, SourceBatch, SourceBatchRequest, SourceMessage, SourceReadCancellation,
};
use nervix_primitives::time::{Instant, sleep};

use super::{
    KafkaGroupCommit,
    commit_tests::{READ_DEADLINE, cluster, publish, read_positions, subscribed_source},
};

/// How long a test lets the consumer fetch before it reads again.
const FETCH_INTERVAL: Duration = Duration::from_millis(10);

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

    let cancellation = SourceReadCancellation::new();
    cancellation.cancel();
    let request = SourceBatchRequest {
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
        let SourceBatch::Interrupted { gathered } = batch else {
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
