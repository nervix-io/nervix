//! A Kafka producer is destroyed on a connector worker, whoever lets its last handle go.
//!
//! Outside the layer order: a harness.
//! - **Owns.** Causal regressions for each owner that can hold a producer's last handle (a sink its
//!   host drops before it publishes or once it finished it, a sink dropped while its broker holds
//!   the answers to the records it sent, a flush that outlives the finish that started it), for a
//!   destruction that waits for the producer's polling thread or meets a full connector queue, and
//!   for the charge a producer holds for its own retirement.
//! - **Depends on.** The connector contract, the bounded executor and librdkafka's mock broker.
//! - **Must not know.** Runtime graph ownership, registry Models or cluster placement.

use std::{
    future::{Future as _, poll_fn},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use error_stack::{AttachmentKind, FrameKind};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerPublishingMode, RecordSink as _, SinkFinish, SinkHost, SinkLifecycle as _, SinkRecordId,
    SinkStartError,
};
use nervix_execution::{AdmissionError, Executor, MemoryClass};
use nervix_primitives::{
    sync::oneshot,
    time::{Instant, timeout},
};
use rdkafka::{mocking::MockCluster, producer::DefaultProducerContext};

use super::{
    KafkaProducer, KafkaProducerStartError, KafkaSink,
    publish_tests::{
        FAST_RECONNECT, PARTITION_ZERO_KEYS, PollingThreadHold, SilentHost, awaited_hand_over,
        cluster, confirmed, record, sink_config, sink_on, small_payload,
    },
};
use crate::retirement_tests::{
    FullConnectorQueue, connector_workers_reach, connector_workers_reached, single_connector_worker,
};

/// A broker round trip no test outlives, so a broker given it holds every answer.
const HELD_ROUND_TRIP: Duration = Duration::from_secs(3600);

/// How long a test waits for the mock broker to answer what the producer sends it.
const ANSWER_DEADLINE: Duration = Duration::from_secs(30);

/// A `NO ACK` sink on `executor` whose producer already reached the broker: it delivered one record
/// and its finish found that record answered.
async fn connected_sink(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    executor: &Executor,
) -> KafkaSink {
    let mut sink = sink_on(
        cluster,
        &FAST_RECONNECT,
        BrokerPublishingMode::NoAck,
        executor.clone(),
    );
    let outcome = sink
        .publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))])
        .await;
    assert_eq!(outcome.into_parts().delivered, vec![SinkRecordId::new(0)]);
    let finished = sink
        .finish(Instant::now() + ANSWER_DEADLINE)
        .await
        .assured("the mock broker answers the first record before the deadline");
    assert_eq!(finished, SinkFinish::Flushed);
    sink
}

/// Hands three records to `sink`'s producer while its broker holds every answer, so they stay in
/// flight.
async fn send_held_records(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    sink: &mut KafkaSink,
) {
    cluster
        .broker_round_trip_time(1, HELD_ROUND_TRIP)
        .assured("the test broker can hold its replies");
    let outcome = sink
        .publish(vec![
            record(1, PARTITION_ZERO_KEYS[0], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[1], small_payload(2)),
            record(3, PARTITION_ZERO_KEYS[0], small_payload(3)),
        ])
        .await;
    assert_eq!(
        outcome.into_parts().delivered,
        vec![
            SinkRecordId::new(1),
            SinkRecordId::new(2),
            SinkRecordId::new(3)
        ]
    );
    assert_eq!(
        sink.retained_records(),
        3,
        "the broker answers none of the records it holds"
    );
}

/// A sink whose emitter's composition fails after the sink started is dropped before it publishes.
#[nervix_primitives::test]
async fn a_sink_dropped_before_it_publishes_destroys_its_producer_on_a_connector_worker() {
    let cluster = cluster(1, 1);
    let executor = Executor::default();
    let sink = sink_on(&cluster, &[], BrokerPublishingMode::NoAck, executor.clone());
    let before = executor.snapshot().connector_storage;

    drop(sink);

    connector_workers_reach(
        &executor,
        "the unused sink's producer is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 1
        },
    )
    .await;
}

/// The host finishes a sink once its emitter publishes nothing more, then drops it when the
/// emitter's task ends, after its loop or when a swap or `ALTER` replaces it.
#[nervix_primitives::test]
async fn a_finished_sink_its_host_drops_destroys_its_producer_on_a_connector_worker() {
    let cluster = cluster(1, 1);
    let executor = Executor::default();
    let mut sink = sink_on(
        &cluster,
        &[],
        confirmed(1, Duration::from_secs(60)),
        executor.clone(),
    );
    let outcome = sink
        .publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))])
        .await;
    assert_eq!(outcome.into_parts().delivered, vec![SinkRecordId::new(0)]);
    let finished = sink
        .finish(Instant::now() + ANSWER_DEADLINE)
        .await
        .assured("a producer whose records were all answered has nothing to send");
    assert_eq!(finished, SinkFinish::NothingHeld);
    let before = executor.snapshot().connector_storage;

    drop(sink);

    connector_workers_reach(
        &executor,
        "the finished sink's producer is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 1
        },
    )
    .await;
}

/// Destroying a producer whose broker holds the answers to the records it sent purges those records
/// and waits for librdkafka to report them, and then for librdkafka's threads.
#[nervix_primitives::test]
async fn a_sink_dropped_while_its_broker_holds_its_records_destroys_its_producer_on_a_connector_worker()
 {
    let cluster = cluster(1, 1);
    let executor = Executor::default();
    let mut sink = connected_sink(&cluster, &executor).await;
    send_held_records(&cluster, &mut sink).await;
    let before = executor.snapshot().connector_storage;

    drop(sink);

    connector_workers_reach(
        &executor,
        "the producer holding unanswered records is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 1
        },
    )
    .await;
}

/// A finish flushes on the runtime's blocking pool through a handle of its own, which keeps the
/// producer for as long as the flush runs, also after the host stopped waiting for the finish and
/// dropped the sink. The flush then lets the last handle go, and a connector job destroys the
/// producer rather than the flush's thread.
#[nervix_primitives::test]
async fn a_flush_that_outlives_its_finish_destroys_the_producer_on_a_connector_worker_once_it_ends()
{
    let cluster = cluster(1, 1);
    let executor = Executor::default();
    let mut sink = connected_sink(&cluster, &executor).await;
    send_held_records(&cluster, &mut sink).await;
    let before = executor.snapshot().connector_storage;
    let mut finishing = Box::pin(sink.finish(Instant::now() + HELD_ROUND_TRIP));
    let first_poll = poll_fn(|context| Poll::Ready(finishing.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "the flush waits for the answers the broker holds"
    );
    drop(finishing);

    drop(sink);
    // The flush cannot end while the broker holds every answer, so it still holds the producer.
    assert_eq!(
        executor.snapshot().connector_storage.admitted,
        before.admitted,
        "nothing destroys the producer the running flush holds"
    );
    // Closing the broker's connections ends the held requests, and the broker answers the records
    // librdkafka sends it again at once, which ends the flush.
    cluster
        .broker_down(1)
        .assured("the test broker can close its connections");
    cluster
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the test releases its broker's later replies");
    cluster
        .broker_up(1)
        .assured("the test broker accepts connections again");

    connector_workers_reach(
        &executor,
        "the producer the flush held last is destroyed by a connector job once the flush ends",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 1
        },
    )
    .await;
}

/// Destroying a producer first joins its polling thread, which a delivery callback can keep. The
/// destruction waits for it on a connector worker, while the task that dropped the sink carries
/// on. A watchdog lets the polling thread go once it saw that, or once the workers' deadline passed
/// without it, so a destruction on the dropping task cannot hold the test forever.
#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_destruction_that_waits_for_the_polling_thread_waits_on_a_connector_worker() {
    let cluster = cluster(1, 1);
    // The broker stays down until the probe's answer is awaited, so the probe's report cannot
    // arrive before the hold's waker is registered for it.
    cluster
        .broker_down(1)
        .assured("the mock cluster takes broker 1 down");
    let executor = Executor::default();
    let sink = sink_on(
        &cluster,
        &FAST_RECONNECT,
        BrokerPublishingMode::NoAck,
        executor.clone(),
    );
    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(0, PARTITION_ZERO_KEYS[0], small_payload(0)));
    let polled = Pin::new(&mut probe).poll(&mut Context::from_waker(&hold.waker));
    assert!(
        polled.is_pending(),
        "no broker is up to answer the probe record yet"
    );
    cluster
        .broker_up(1)
        .assured("the mock cluster brings broker 1 up");
    timeout(ANSWER_DEADLINE, hold.held)
        .await
        .assured("the polling thread serves the probe record's delivery report in time")
        .assured("the hold tells the test the thread it holds");
    let before = executor.snapshot().connector_storage;
    let watched = executor.clone();
    let release = hold.release;
    let (watching, has_started) = oneshot::channel();
    let watchdog = nervix_primitives::task::spawn(async move {
        watching
            .send(())
            .assured("the test awaits the watchdog before it drops the sink");
        let waited_on_a_worker = connector_workers_reached(&watched, |workers| {
            workers.admitted == before.admitted + 1 && workers.running == before.running + 1
        })
        .await;
        drop(release);
        waited_on_a_worker
    });
    has_started
        .await
        .assured("the watchdog runs before the sink is dropped");

    drop(sink);

    let waited_on_a_worker = watchdog.await.assured("the watchdog's task is joined");
    assert!(
        waited_on_a_worker,
        "the destruction waits for the held polling thread on a connector worker"
    );
    connector_workers_reach(
        &executor,
        "the destruction ends once the polling thread is let go",
        |workers| workers.completed == before.completed + 1 && workers.running == before.running,
    )
    .await;
}

/// A producer whose last handle is gone is work the node already holds, which nothing can present
/// again, so a full connector queue keeps its destruction waiting for a place instead of refusing
/// it.
#[nervix_primitives::test]
async fn a_producer_destruction_waits_for_a_place_in_a_full_connector_queue() {
    let cluster = cluster(1, 1);
    let executor = single_connector_worker();
    let sink = sink_on(&cluster, &[], BrokerPublishingMode::NoAck, executor.clone());
    let full = FullConnectorQueue::fill(&executor).await;
    let before = executor.snapshot().connector_storage;

    drop(sink);
    full.release().await;

    connector_workers_reach(
        &executor,
        "the producer is destroyed by a connector job once a queue place frees",
        |workers| {
            workers.admitted == before.admitted + 1
                && workers.refused == before.refused
                && workers.completed == before.completed + 3
        },
    )
    .await;
}

/// A producer takes the charge of the job that will destroy it before it exists, so a node whose
/// commands budget has no room refuses to start the sink, and its retirement gives that charge
/// back.
#[nervix_primitives::test]
async fn a_producer_holds_the_charge_of_its_retirement_from_its_creation() {
    let cluster = cluster(1, 1);
    let executor = Executor::default();
    let capacity = executor.snapshot().commands_memory.capacity_bytes;
    let occupied = executor
        .try_reserve(MemoryClass::Commands, capacity)
        .assured("the test occupies the command budget before starting the sink");

    let refused = KafkaSink::new(
        sink_config(&cluster, &[], BrokerPublishingMode::NoAck),
        SinkHost::new(SilentHost::on(executor.clone())),
    );

    let Err(report) = refused else {
        panic!("a full commands budget refuses the producer's retirement charge");
    };
    assert_eq!(
        report.current_context(),
        &SinkStartError::Initialize { sink: "kafka" }
    );
    assert_eq!(
        report.downcast_ref::<KafkaProducerStartError>(),
        Some(&KafkaProducerStartError::RetirementAdmission)
    );
    let mut description = None;
    for frame in report.frames() {
        if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
            description = Some(attachment.to_string());
            break;
        }
    }
    let description = description.assured("the refusal is described for the emitter's status");
    assert!(
        description.starts_with("the KAFKA producer cannot reserve the job that will destroy it: "),
        "the emitter's status names the refused charge and its cause: {description}"
    );
    assert!(matches!(
        report.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted {
            class: "commands",
            ..
        })
    ));
    drop(occupied);
    let sink = sink_on(&cluster, &[], BrokerPublishingMode::NoAck, executor.clone());
    let charge = u64::try_from(std::mem::size_of::<KafkaProducer>())
        .assured("a producer handle's size fits in u64");
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, charge);
    let before = executor.snapshot().connector_storage;

    drop(sink);

    connector_workers_reach(
        &executor,
        "the producer is destroyed by a connector job",
        |workers| workers.completed == before.completed + 1,
    )
    .await;
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
}
