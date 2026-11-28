//! When a held Kafka consumer's group service is due: half of librdkafka's `max.poll.interval.ms`
//! after the consumer's last poll, the cadence on which rust-rdkafka's stream wakes a reader that
//! waits for records. The reads run against librdkafka's in-process mock cluster.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, IngestorBatch, IngestorBatchRequest, IngestorConnector,
    IngestorReadCancellation, IngestorResume,
};
use nervix_execution::Executor;
use nervix_models::ClientConfigEntry;
use nervix_primitives::time::{Instant, timeout};
use rdkafka::config::ClientConfig;

use super::{
    super::{
        KafkaGroupCommit, KafkaTopicIngestor,
        commit_tests::{READ_DEADLINE, cluster, host_on, plan, publish, read_positions},
    },
    KafkaAllowanceHold,
};

/// The instant `delay` after `instant`, for a test whose delays all fit the monotonic clock.
fn after(instant: Instant, delay: Duration) -> Instant {
    instant
        .checked_add(delay)
        .assured("half of librdkafka's longest poll interval, one day, fits the monotonic clock")
}

#[nervix_primitives::test]
async fn the_group_service_is_due_half_a_poll_interval_after_the_last_poll() {
    // librdkafka accepts a poll interval from 1 to 86400000 milliseconds, 300000 when unset.
    let intervals = [
        (Some("1"), Duration::from_micros(500)),
        (None, Duration::from_secs(150)),
        (Some("86400000"), Duration::from_secs(43_200)),
    ];
    for (interval, half) in intervals {
        let mut config = ClientConfig::new();
        if let Some(interval) = interval {
            config.set("max.poll.interval.ms", interval);
        }
        let mut hold = KafkaAllowanceHold::new(&config)
            .assured("librdkafka accepts every poll interval within its range");
        assert_eq!(
            hold.next_service, None,
            "a consumer that has not polled since it subscribed is serviced as soon as it is held"
        );

        let before = Instant::now();
        hold.polled();
        let polled = Instant::now();
        let due = hold
            .next_service
            .assured("a recorded poll makes the next service due");
        assert!(
            due >= after(before, half) && due <= after(polled, half),
            "with max.poll.interval.ms {interval:?} the service is due {half:?} after the poll"
        );
    }
}

#[nervix_primitives::test]
async fn a_read_that_waits_out_its_batch_deadline_keeps_its_record_poll_for_the_service() {
    const POLL_INTERVAL: Duration = Duration::from_secs(6);
    const BATCH_TIMEOUT: Duration = Duration::from_secs(2);

    let broker = cluster();
    publish(&broker).await;
    let mut plan = plan(&broker, KafkaGroupCommit::EachAcknowledgedBatch);
    for key in ["session.timeout.ms", "max.poll.interval.ms"] {
        plan.config.push(ClientConfigEntry {
            key: key.to_owned(),
            value: POLL_INTERVAL.as_millis().to_string(),
        });
    }
    let executor = Executor::default();
    let host = host_on(&executor);
    let mut ingestor = KafkaTopicIngestor::open(&plan, &host, 0)
        .await
        .assured("the held consumer accepts its group configuration");
    let resumed = ingestor
        .resume(&host)
        .await
        .assured("the group subscribes to the broker's topic");
    assert!(matches!(resumed, IngestorResume::Ready));
    let joined = read_positions(&mut ingestor, &host).await;
    assert_eq!(
        joined.len(),
        1,
        "the group's first read takes the first record"
    );

    publish(&broker).await;
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(8).assured("eight is a nonzero batch bound"),
        batch_timeout: Some(BATCH_TIMEOUT),
    };
    let batch = timeout(
        READ_DEADLINE,
        ingestor.next_batch(&host, request, &IngestorReadCancellation::new()),
    )
    .await
    .assured("the consumer reads the second record within the test observation deadline")
    .assured("an uncancelled read of a stored record succeeds");
    let ended = Instant::now();
    let IngestorBatch::Messages(messages) = batch else {
        panic!("an uncancelled read returns its gathered messages");
    };
    assert_eq!(
        messages.len(),
        1,
        "the read gathers the one stored record and then waits out its batch deadline"
    );

    // The record's poll came at least the batch deadline before the read ended, and nothing
    // polled the consumer after that the ingestor could observe.
    let latest_record_poll = ended
        .checked_sub(BATCH_TIMEOUT)
        .assured("the read waited its batch deadline after the monotonic clock began");
    let due = ingestor
        .allowance_hold
        .as_ref()
        .assured("a consumer-group member services its group while it is held")
        .next_service
        .assured("the read's poll makes the next service due");
    assert!(
        due <= after(latest_record_poll, POLL_INTERVAL / 2),
        "the service is due half a poll interval after the record's poll, not after the read's \
         batch deadline"
    );
}
