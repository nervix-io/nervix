//! A Kafka consumer is destroyed on a connector worker, whoever lets its last handle go.
//!
//! Outside the layer order: a harness.
//! - **Owns.** Causal regressions for each owner that can hold a consumer's last handle (the source
//!   its host drops, a partition inspector, a control job that outlives its caller, a commit
//!   refused or answered after its source is gone), for a retirement that meets a full connector
//!   queue, and for the charge a consumer holds for its own retirement.
//! - **Depends on.** The connector contract, the bounded executor and librdkafka's mock broker.
//! - **Must not know.** Runtime graph ownership, registry Models or cluster placement.

use std::{
    future::{Future as _, poll_fn},
    num::NonZeroUsize,
    task::Poll,
    time::Duration,
};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_connector::SourceConnector as _;
use nervix_execution::{
    AdmissionError, ConnectorStorageWorkers, ExecutionConfig, ExecutionError, Executor,
    MemoryClass, StorageClass, WorkerClassSnapshot, WorkerCounts,
};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use rdkafka::{consumer::StreamConsumer, mocking::MockCluster, producer::DefaultProducerContext};

use super::{
    GroupCommitRequest, KafkaGroupCommit, KafkaOffsetPosition, KafkaSource, KafkaSourceError,
    KafkaSourcePlan, TopicPartitionInspector,
    commit_tests::{GROUP, HELD_ROUND_TRIP, READ_DEADLINE, cluster, plan, publish, read_positions},
};

/// How often a test reads the connector workers' counters while it waits for a retirement.
const COUNTER_POLL: Duration = Duration::from_millis(10);

/// The plan of a source in the test group that runs its driver jobs on `executor`.
fn plan_on(
    broker: &MockCluster<'static, DefaultProducerContext>,
    executor: &Executor,
) -> KafkaSourcePlan {
    let mut plan = plan(broker, KafkaGroupCommit::EachAcknowledgedBatch);
    plan.executor = executor.clone();
    plan
}

/// An executor with one connector worker and one place in the connector queue.
fn single_connector_worker() -> Executor {
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            connector_storage: ConnectorStorageWorkers::resolve(Some(NonZeroUsize::MIN))
                .assured("one worker fits the primitive permit bound"),
            pending_jobs: NonZeroUsize::MIN,
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only worker counts")
}

/// Waits until the connector workers' counters show `reached`, and fails naming `awaited` once the
/// test's read deadline passes.
async fn connector_workers_reach(
    executor: &Executor,
    awaited: &str,
    reached: impl Fn(&WorkerClassSnapshot) -> bool,
) {
    let waited = timeout(READ_DEADLINE, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if reached(&executor.snapshot().connector_storage) {
                return;
            }
            sleep(COUNTER_POLL).await;
        }
    })
    .await;
    assert!(
        waited.is_ok(),
        "{awaited}; connector workers: {:?}",
        executor.snapshot().connector_storage
    );
}

type ConnectorJob = JoinHandle<Result<(), Report<ExecutionError>>>;

/// The only connector worker, held by a job the test releases, and the only place in the queue,
/// taken by a job waiting behind it.
struct FullConnectorQueue {
    release: oneshot::Sender<()>,
    occupying: ConnectorJob,
    queued: ConnectorJob,
}

impl FullConnectorQueue {
    async fn fill(executor: &Executor) -> Self {
        let (started, has_started) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        let occupier = executor.clone();
        let charge = executor
            .try_reserve(MemoryClass::Commands, 1)
            .assured("the occupying job's envelope fits");
        let occupying = nervix_primitives::task::spawn(async move {
            occupier
                .run_storage(StorageClass::Connector, charge, move |_, _| {
                    started.send(()).assured("the test awaits worker admission");
                    released
                        .blocking_recv()
                        .assured("the test releases the only connector worker");
                })
                .await
        });
        has_started
            .await
            .assured("the occupying job holds the only connector worker");
        let waiter = executor.clone();
        let charge = executor
            .try_reserve(MemoryClass::Commands, 1)
            .assured("the queued job's envelope fits");
        let queued = nervix_primitives::task::spawn(async move {
            waiter
                .run_storage(StorageClass::Connector, charge, |_, _| ())
                .await
        });
        connector_workers_reach(
            executor,
            "the second job takes the only queue place",
            |workers| workers.pending == 1,
        )
        .await;
        Self {
            release,
            occupying,
            queued,
        }
    }

    async fn release(self) {
        self.release
            .send(())
            .assured("the occupying job waits for its release");
        self.occupying
            .await
            .assured("the occupying job's supervisor is joined")
            .assured("the occupying job exits");
        self.queued
            .await
            .assured("the queued job's supervisor is joined")
            .assured("the queued job runs once the worker is free");
    }
}

/// A source's own drop, at the end of its host's loop, lets the last handle go. Its consumer joined
/// the group, so destroying it waits for the coordinator to answer its LeaveGroup request, which
/// the held broker never does: that wait occupies a connector worker, not the dropping task.
#[nervix_primitives::test]
async fn a_dropped_source_destroys_its_joined_consumer_on_a_connector_worker() {
    let broker = cluster();
    publish(&broker).await;
    let executor = Executor::default();
    let mut source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source accepts its group and broker configuration");
    source
        .resume()
        .await
        .assured("the source subscribes to its topic");
    read_positions(&mut source).await;
    source
        .close()
        .await
        .assured("the source unsubscribes before its host drops it");
    broker
        .broker_round_trip_time(1, HELD_ROUND_TRIP)
        .assured("the test broker can hold its replies");
    let before = executor.snapshot().connector_storage;

    drop(source);

    connector_workers_reach(
        &executor,
        "the dropped consumer's destruction waits for its LeaveGroup answer on a connector worker",
        |workers| workers.admitted == before.admitted + 1 && workers.running == 1,
    )
    .await;
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the test releases its broker's later replies");
    connector_workers_reach(
        &executor,
        "the destruction ends once its LeaveGroup request times out",
        |workers| workers.completed == before.completed + 1 && workers.running == 0,
    )
    .await;
}

/// A partition inspector lets the last handle go when its watch ends.
#[nervix_primitives::test]
async fn a_dropped_partition_inspector_destroys_its_consumer_on_a_connector_worker() {
    let broker = cluster();
    let executor = Executor::default();
    let inspector = TopicPartitionInspector::new(
        &[ClientConfigEntry {
            key: "bootstrap.servers".to_string(),
            value: broker.bootstrap_servers(),
        }],
        "orders-inspector".to_string(),
        executor.clone(),
    )
    .assured("the inspector accepts the test broker's configuration");
    inspector
        .partitions("orders")
        .await
        .assured("the mock broker serves the test topic's metadata");
    let before = executor.snapshot().connector_storage;

    drop(inspector);

    connector_workers_reach(
        &executor,
        "the dropped inspector's consumer is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 1
        },
    )
    .await;
}

/// A consumer whose last handle is gone is work the node already holds, which nothing can present
/// again, so a full connector queue keeps its destruction waiting for a place instead of refusing
/// it.
#[nervix_primitives::test]
async fn a_destruction_waits_for_a_place_in_a_full_connector_queue() {
    let broker = cluster();
    let executor = single_connector_worker();
    let source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source accepts its group and broker configuration");
    let full = FullConnectorQueue::fill(&executor).await;
    let before = executor.snapshot().connector_storage;

    drop(source);
    full.release().await;

    connector_workers_reach(
        &executor,
        "the consumer is destroyed by a connector job once a queue place frees",
        |workers| {
            workers.admitted == before.admitted + 1
                && workers.refused == before.refused
                && workers.completed == before.completed + 3
        },
    )
    .await;
}

/// A commit's job carries a handle of its own. When the node refuses that job after the commit's
/// caller and its source are gone, the refused job let the last handle go.
#[nervix_primitives::test]
async fn a_commit_refused_after_its_source_was_dropped_destroys_the_consumer_on_a_connector_worker()
{
    let broker = cluster();
    let executor = single_connector_worker();
    let mut source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source accepts its group and broker configuration");
    let full = FullConnectorQueue::fill(&executor).await;
    let before = executor.snapshot().connector_storage;
    let request = GroupCommitRequest::acknowledged(&[KafkaOffsetPosition {
        topic: "orders".to_string(),
        partition: 0,
        offset: 1,
    }])
    .assured("the position has a valid next offset");
    let mut commit = Box::pin(source.commits.commit(
        &source.control,
        &source.consumer,
        GROUP,
        request,
    ));
    let first_poll = poll_fn(|context| Poll::Ready(commit.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "the commit waits for its job, which has not asked for a place yet"
    );
    drop(commit);

    drop(source);

    connector_workers_reach(
        &executor,
        "the full connector queue refuses the commit's job",
        |workers| workers.refused == before.refused + 1,
    )
    .await;
    full.release().await;
    connector_workers_reach(
        &executor,
        "the consumer the refused commit held last is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 1 && workers.completed == before.completed + 3
        },
    )
    .await;
}

/// A commit outlives its source when its answer comes late. Its job keeps the consumer for as long
/// as the commit waits, and the consumer is destroyed by a job of its own once the commit's job
/// lets the last handle go.
#[nervix_primitives::test]
async fn a_commit_answered_after_its_source_was_dropped_keeps_the_consumer_until_it_ends() {
    let broker = cluster();
    publish(&broker).await;
    let executor = Executor::default();
    let mut source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source accepts its group and broker configuration");
    source
        .resume()
        .await
        .assured("the source subscribes to its topic");
    let positions = read_positions(&mut source).await;
    broker
        .broker_round_trip_time(1, HELD_ROUND_TRIP)
        .assured("the test broker can hold its replies");
    let before = executor.snapshot().connector_storage;
    let request = GroupCommitRequest::acknowledged(&positions)
        .assured("the delivered record has a valid next offset");
    let mut commit = Box::pin(source.commits.commit(
        &source.control,
        &source.consumer,
        GROUP,
        request,
    ));
    let first_poll = poll_fn(|context| Poll::Ready(commit.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "the held broker has not answered");
    connector_workers_reach(
        &executor,
        "the held commit runs on a connector worker",
        |workers| workers.admitted == before.admitted + 1 && workers.running == 1,
    )
    .await;
    drop(commit);

    drop(source);
    // The commit's job now holds the last handle. Closing the broker's connections ends the held
    // request, and the broker answers whatever librdkafka sends it next at once.
    broker
        .broker_down(1)
        .assured("the test broker can close its connections");
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the test releases its broker's later replies");
    broker
        .broker_up(1)
        .assured("the test broker accepts connections again");

    connector_workers_reach(
        &executor,
        "the consumer is destroyed by a connector job once its late commit ends",
        |workers| {
            workers.admitted == before.admitted + 2
                && workers.completed == before.completed + 2
                && workers.running == 0
        },
    )
    .await;
}

/// A control job keeps running when its caller stops waiting for it. Once its source is gone, the
/// job holds the last handle until its native operation returns.
#[nervix_primitives::test]
async fn a_control_job_that_outlives_its_source_destroys_the_consumer_on_a_connector_worker() {
    let broker = cluster();
    let executor = Executor::default();
    let source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source accepts its group and broker configuration");
    let before = executor.snapshot().connector_storage;
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let mut running = Box::pin(source.control.run(&source.consumer, move |_, _| {
        started
            .send(())
            .assured("the test awaits the native operation");
        released
            .blocking_recv()
            .assured("the test releases the operation that outlives its caller");
        Ok(())
    }));
    let first_poll = poll_fn(|context| Poll::Ready(running.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "the native operation is held");
    has_started
        .await
        .assured("the control job runs on a connector worker");
    drop(running);

    drop(source);
    release
        .send(())
        .assured("the control job waits for its release");

    connector_workers_reach(
        &executor,
        "the consumer the outliving control job held last is destroyed by a connector job",
        |workers| {
            workers.admitted == before.admitted + 2 && workers.completed == before.completed + 2
        },
    )
    .await;
}

/// A consumer takes the charge of the job that will destroy it before it exists, so a node whose
/// commands budget has no room refuses to create it, and its retirement gives that charge back.
#[nervix_primitives::test]
async fn a_consumer_holds_the_charge_of_its_retirement_from_its_creation() {
    let broker = cluster();
    let executor = Executor::default();
    let capacity = executor.snapshot().commands_memory.capacity_bytes;
    let occupied = executor
        .try_reserve(MemoryClass::Commands, capacity)
        .assured("the test occupies the command budget before opening");

    let refused = KafkaSource::open(&plan_on(&broker, &executor), 0).await;

    let Err(report) = refused else {
        panic!("a full commands budget refuses the consumer's retirement charge");
    };
    assert!(matches!(
        report.downcast_ref::<KafkaSourceError>(),
        Some(KafkaSourceError::RetirementAdmission)
    ));
    assert!(matches!(
        report.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted {
            class: "commands",
            ..
        })
    ));
    drop(occupied);
    let source = KafkaSource::open(&plan_on(&broker, &executor), 0)
        .await
        .assured("the source opens once the budget has room");
    let charge = u64::try_from(std::mem::size_of::<StreamConsumer>())
        .assured("a consumer handle's size fits in u64");
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, charge);
    let before = executor.snapshot().connector_storage;

    drop(source);

    connector_workers_reach(
        &executor,
        "the consumer is destroyed by a connector job",
        |workers| workers.completed == before.completed + 1,
    )
    .await;
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
}
