//! The Kafka ingestor yields its async worker while a broker holds a control request.
//!
//! Outside the layer order: a harness.
//! - **Owns.** Causal driver-wait and cancelled-commit regressions through the production ingestor.
//! - **Depends on.** The connector contract, bounded executor and librdkafka's mock broker.
//! - **Must not know.** Runtime graph ownership, registry Models or cluster placement.

use std::{
    future::{Future as _, poll_fn},
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use meticulous::ResultExt as _;
use nervix_connector::{
    BrokerIngestorConnector as _, DomainOffsetInitialization, DomainOffsetPosition,
    DomainOffsetResult, DomainOffsetServices, DomainOffsets, IngestorConnector,
};
use nervix_execution::{AdmissionError, Executor, MemoryClass, StorageClass};
use nervix_models::{ClientConfigEntry, KafkaTopicName};
use nervix_primitives::sync::oneshot;
use rdkafka::{mocking::MockCluster, producer::DefaultProducerContext, types::RDKafkaRespErr};

use super::{
    GroupCommitRequest, KafkaGroupCommit, KafkaTopicIngestor, KafkaTopicIngestorError,
    KafkaTopicIngestorOffsetMode, KafkaTopicIngestorPlan, TopicPartitionInspector,
    commit_tests::{
        READ_DEADLINE, cluster, plan as group_plan, publish, read_positions, subscribed_ingestor,
    },
};
use crate::topic::retirement_tests::{BusyConnectorWorker, single_connector_worker};

struct UnreachedOffsets;

#[nervix_primitives::test]
async fn an_admitted_partition_inspection_preserves_a_named_missing_topic() {
    let broker = cluster();
    broker
        .topic_error(
            "orders",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        )
        .assured("the test broker reports its topic as missing");
    let plan = group_plan(&broker, KafkaGroupCommit::EachAcknowledgedBatch);
    let inspector = TopicPartitionInspector::new(
        &plan.config,
        "metadata-missing-topic".to_string(),
        plan.executor,
    )
    .await
    .assured("the inspector accepts the test broker configuration");

    let report = inspector
        .partitions("orders")
        .await
        .expect_err("the broker reports the topic as missing");

    assert!(matches!(
        report.current_context(),
        KafkaTopicIngestorError::MissingTopic { topic } if topic == "orders"
    ));
    broker
        .topic_error("orders", RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR)
        .assured("the test broker now exposes the provisioned topic");
    assert_eq!(
        inspector
            .partitions("orders")
            .await
            .assured("the same inspector observes the provisioned topic"),
        vec![0]
    );
}

#[nervix_primitives::test]
async fn rejecting_a_delivered_record_admits_its_seek_and_reads_that_offset_again() {
    let broker = cluster();
    publish(&broker).await;
    let executor = Executor::default();
    let mut plan = group_plan(&broker, KafkaGroupCommit::EachAcknowledgedBatch);
    plan.executor = executor.clone();
    let mut ingestor = KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("the ingestor accepts its group and broker configuration");
    assert!(matches!(
        ingestor
            .resume()
            .await
            .assured("the ingestor subscribes to its topic"),
        nervix_connector::IngestorResume::Ready
    ));
    let positions = read_positions(&mut ingestor).await;
    let admitted = executor.snapshot().connector_storage.admitted;

    ingestor
        .reject(&positions)
        .await
        .assured("the rejected partition is still assigned");

    assert_eq!(
        executor.snapshot().connector_storage.admitted,
        admitted + 1,
        "all rejected partitions seek through one admitted control job"
    );
    assert_eq!(read_positions(&mut ingestor).await, positions);
}

#[async_trait]
impl DomainOffsetServices for UnreachedOffsets {
    fn generation(&self) -> Option<u64> {
        Some(1)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> DomainOffsetResult<DomainOffsetInitialization> {
        panic!("the held broker has not returned the partitions yet")
    }

    async fn reset(
        &self,
        _generation: u64,
        _positions: Vec<DomainOffsetPosition>,
    ) -> DomainOffsetResult<()> {
        panic!("the held broker has not returned the partitions yet")
    }

    async fn commit(&self, _position: DomainOffsetPosition) -> DomainOffsetResult<()> {
        panic!("the held broker has delivered no message")
    }
}

#[nervix_primitives::test]
async fn cancelling_a_commit_keeps_its_request_in_flight() {
    let broker = cluster();
    publish(&broker).await;
    let mut ingestor = subscribed_ingestor(&broker, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let positions = read_positions(&mut ingestor).await;
    broker
        .broker_round_trip_time(1, Duration::from_secs(3600))
        .assured("the test broker can hold its replies");
    let request = GroupCommitRequest::positions(&positions)
        .assured("the delivered record has a valid next offset");
    let mut commit = Box::pin(ingestor.commits.commit(
        &ingestor.control,
        &ingestor.consumer,
        "orders-ingestor",
        request,
    ));
    let first_poll = poll_fn(|context| Poll::Ready(commit.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "the held broker has not answered");
    drop(commit);
    let retained = ingestor.commits.unanswered.is_some();
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the test releases its broker before checking cancellation");

    assert!(
        retained,
        "a cancelled caller retains its accepted commit before another commit can start"
    );
}

#[nervix_primitives::test]
async fn a_control_turn_outlives_the_future_that_admitted_it() {
    let broker = cluster();
    let ingestor = domain_ingestor(&broker, Executor::default()).await;
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    {
        let operation = ingestor.control.run(&ingestor.consumer, move |_, _| {
            started
                .send(())
                .assured("the test observes worker admission");
            released
                .blocking_recv()
                .assured("the test releases the admitted operation after dropping its future");
            Ok(())
        });
        nervix_primitives::select! {
            result = operation => panic!("the admitted job is still held: {result:?}"),
            result = has_started => result.assured("the admitted worker holds the control turn"),
        }
    }

    let mut next_turn = std::pin::pin!(ingestor.control.wait_for_turn());
    let status = poll_fn(|context| Poll::Ready(next_turn.as_mut().poll(context))).await;
    assert!(status.is_pending(), "the admitted job still owns the turn");
    release.send(()).assured("the admitted job awaits release");
    nervix_primitives::time::timeout(READ_DEADLINE, next_turn)
        .await
        .assured("the next turn opens when the admitted job ends")
        .assured("the control turn became free within its deadline");
}

#[nervix_primitives::test]
async fn a_domain_resume_yields_while_the_broker_holds_its_metadata() {
    let broker = cluster();
    broker
        .broker_round_trip_time(1, Duration::from_secs(3600))
        .assured("the test broker can hold its replies");
    let mut ingestor = domain_ingestor(&broker, Executor::default()).await;
    let mut resume = Box::pin(ingestor.resume());
    let first_poll = poll_fn(|context| Poll::Ready(resume.as_mut().poll(context))).await;
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the broker releases its replies before the test drops its ingestor");

    assert!(
        first_poll.is_pending(),
        "resuming yields its async worker before waiting for the broker; got {first_poll:?}"
    );
}

async fn domain_ingestor(
    broker: &MockCluster<'static, DefaultProducerContext>,
    executor: Executor,
) -> KafkaTopicIngestor {
    let plan = KafkaTopicIngestorPlan {
        settings: crate::KafkaTopicIngestorSettings::default(),
        executor,
        config: vec![
            ClientConfigEntry {
                key: "bootstrap.servers".to_string(),
                value: broker.bootstrap_servers(),
            },
            ClientConfigEntry {
                key: "socket.timeout.ms".to_string(),
                value: "1000".to_string(),
            },
        ],
        topic: KafkaTopicName::parse("orders").assured("orders is a valid Kafka topic"),
        offset_mode: KafkaTopicIngestorOffsetMode::Domain {
            group_id: "metadata-worker-yield".to_string(),
            offsets: DomainOffsets::new(UnreachedOffsets),
        },
    };
    KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("the ingestor accepts the test broker's configuration")
}

/// A full command budget holds the metadata job back rather than refusing it: the client's own
/// control deadline bounds the wait and ends it as a timeout, no memory refusal is beneath it,
/// and no connector job ran meanwhile.
#[nervix_primitives::test]
async fn a_full_command_budget_holds_the_control_job_back_until_its_deadline() {
    let broker = cluster();
    let executor = Executor::default();
    let mut ingestor = domain_ingestor(&broker, executor.clone()).await;
    // The open ingestor already holds the charge of its consumer's retirement.
    let commands = executor.snapshot().commands_memory;
    let _held = executor
        .try_reserve(
            MemoryClass::Commands,
            commands.capacity_bytes - commands.reserved_bytes,
        )
        .assured("the test occupies the rest of the command budget before resuming");
    let report = ingestor
        .resume()
        .await
        .expect_err("the control deadline ends the metadata job's wait for room");
    assert!(
        matches!(
            report.downcast_ref::<KafkaTopicIngestorError>(),
            Some(KafkaTopicIngestorError::ControlTimeout { .. })
        ),
        "{report:?}"
    );
    assert!(
        report.downcast_ref::<AdmissionError>().is_none(),
        "a full class held the job back without refusing it: {report:?}"
    );
    assert_eq!(executor.snapshot().connector_storage.admitted, 0);
}

/// A control job that finds the only connector worker busy waits for it rather than being refused,
/// and runs once it frees. The client deadline still bounds the whole wait: a job whose worker stays
/// held past it ends without starting its native operation.
#[nervix_primitives::test]
async fn control_admission_waits_for_a_place_and_spends_the_client_deadline_on_it() {
    let broker = cluster();
    let executor = single_connector_worker();
    let ingestor = domain_ingestor(&broker, executor.clone()).await;
    // The charge of its consumer's retirement, which the open ingestor holds throughout.
    let opened = executor.snapshot().commands_memory.reserved_bytes;

    let full = BusyConnectorWorker::fill(&executor).await;
    let mut waiting = Box::pin(ingestor.control.run(&ingestor.consumer, |_, _| Ok(())));
    let first_poll = poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "the busy connector worker holds the control job back"
    );
    full.release().await;
    waiting
        .await
        .assured("the control job runs once the worker frees, within the client deadline");

    // Now the only worker stays held past the deadline. This wait must spend the effective client
    // deadline on admission without starting the native operation afterwards.
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let occupied = executor.clone();
    let charge = executor
        .try_reserve(MemoryClass::Commands, 1)
        .assured("the occupying envelope fits");
    let running = nervix_primitives::task::spawn(async move {
        occupied
            .run_storage(StorageClass::Connector, charge, move |_, _| {
                started.send(()).assured("the test awaits worker admission");
                released
                    .blocking_recv()
                    .assured("the test releases its occupying job");
            })
            .await
    });
    has_started
        .await
        .assured("the occupying job holds the only connector worker");
    let expired: Result<(), _> = ingestor
        .control
        .run(&ingestor.consumer, |_, _| {
            panic!("a control job whose admission deadline expired cannot start")
        })
        .await;
    release
        .send(())
        .assured("the test releases its worker before checking the timeout");
    running
        .await
        .assured("the supervisor is joined")
        .assured("the occupying job exits");

    let report = expired.expect_err("the configured deadline includes waiting for a worker");
    assert!(matches!(report.current_context(),
        KafkaTopicIngestorError::ControlTimeout { timeout } if *timeout == Duration::from_secs(1)));
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, opened);
}

#[nervix_primitives::test]
async fn a_cancelled_control_call_finishes_before_the_next_can_mutate_its_consumer() {
    let broker = cluster();
    let executor = Executor::default();
    let ingestor = domain_ingestor(&broker, executor.clone()).await;
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut first = Box::pin(ingestor.control.run(&ingestor.consumer, move |_, _| {
        started
            .send(())
            .assured("the test awaits the native operation");
        released
            .blocking_recv()
            .assured("the test releases the operation that outlives its caller");
        Ok(())
    }));
    let poll = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
    assert!(poll.is_pending());
    has_started
        .await
        .assured("the first native operation is in flight");
    drop(first);

    let mut second = Box::pin(ingestor.control.run(&ingestor.consumer, |_, _| Ok(())));
    let poll = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
    let admitted_before_release = executor.snapshot().connector_storage.admitted;
    release
        .send(())
        .assured("the first operation remains held until the second is polled");
    assert!(poll.is_pending());
    assert_eq!(
        admitted_before_release, 1,
        "cancelling a caller cannot let another operation overtake its native mutation"
    );
    second
        .await
        .assured("the next operation runs after its predecessor releases the consumer");
}
