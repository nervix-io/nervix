//! The Kafka source yields its async worker while a broker holds a control request.
//!
//! Outside the layer order: a harness.
//! - **Owns.** Causal driver-wait and cancelled-commit regressions through the production source.
//! - **Depends on.** The connector contract, bounded executor and librdkafka's mock broker.
//! - **Must not know.** Runtime graph ownership, registry Models or cluster placement.

use std::{
    future::{Future as _, poll_fn},
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use meticulous::ResultExt as _;
use nervix_connector::{BrokerSourceConnector as _, SourceConnector};
use nervix_execution::{
    AdmissionError, ExecutionConfig, ExecutionError, Executor, MemoryClass, StorageClass,
    WorkerCounts,
};
use nervix_models::{ClientConfigEntry, KafkaTopicName};
use nervix_primitives::sync::{oneshot, watch};
use rdkafka::{mocking::MockCluster, producer::DefaultProducerContext, types::RDKafkaRespErr};

use super::{
    GroupCommitRequest, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaGroupCommit, KafkaOffsetPosition,
    KafkaSource, KafkaSourceError, KafkaSourceOffsetMode, KafkaSourcePlan, TopicPartitionInspector,
    commit_tests::{cluster, plan as group_plan, publish, read_positions, subscribed_source},
};

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
    .assured("the inspector accepts the test broker configuration");

    let report = inspector
        .partitions("orders")
        .await
        .expect_err("the broker reports the topic as missing");

    assert!(matches!(
        report.current_context(),
        KafkaSourceError::MissingTopic { topic } if topic == "orders"
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
    let mut source = KafkaSource::open(&plan, 0)
        .await
        .assured("the source accepts its group and broker configuration");
    assert!(matches!(
        source
            .resume()
            .await
            .assured("the source subscribes to its topic"),
        nervix_connector::SourceResume::Ready
    ));
    let positions = read_positions(&mut source).await;
    let admitted = executor.snapshot().connector_storage.admitted;

    source
        .reject(&positions)
        .await
        .assured("the rejected partition is still assigned");

    assert_eq!(
        executor.snapshot().connector_storage.admitted,
        admitted + 1,
        "all rejected partitions seek through one admitted control job"
    );
    assert_eq!(read_positions(&mut source).await, positions);
}

#[async_trait]
impl KafkaDomainOffsetServices for UnreachedOffsets {
    fn generation(&self) -> Option<u64> {
        Some(1)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        panic!("the held broker has not returned the partitions yet")
    }

    async fn reset(&self, _positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()> {
        panic!("the held broker has not returned the partitions yet")
    }

    async fn commit(&self, _position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        panic!("the held broker has delivered no message")
    }
}

#[nervix_primitives::test]
async fn cancelling_a_commit_keeps_its_request_in_flight() {
    let broker = cluster();
    publish(&broker).await;
    let mut source = subscribed_source(&broker, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let positions = read_positions(&mut source).await;
    broker
        .broker_round_trip_time(1, Duration::from_secs(3600))
        .assured("the test broker can hold its replies");
    let request = GroupCommitRequest::acknowledged(&positions)
        .assured("the delivered record has a valid next offset");
    let mut commit = Box::pin(source.commits.commit(
        &source.control,
        &source.consumer,
        "orders-ingestor",
        request,
    ));
    let first_poll = poll_fn(|context| Poll::Ready(commit.as_mut().poll(context))).await;
    assert!(first_poll.is_pending(), "the held broker has not answered");
    drop(commit);
    let retained = source.commits.unanswered.is_some();
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the test releases its broker before checking cancellation");

    assert!(
        retained,
        "a cancelled caller retains its accepted commit before another commit can start"
    );
}

#[nervix_primitives::test]
async fn a_domain_resume_yields_while_the_broker_holds_its_metadata() {
    let broker = cluster();
    broker
        .broker_round_trip_time(1, Duration::from_secs(3600))
        .assured("the test broker can hold its replies");
    let mut source = domain_source(&broker, Executor::default()).await;
    let mut resume = Box::pin(source.resume());
    let first_poll = poll_fn(|context| Poll::Ready(resume.as_mut().poll(context))).await;
    broker
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the broker releases its replies before the test drops its source");

    assert!(
        first_poll.is_pending(),
        "resuming yields its async worker before waiting for the broker; got {first_poll:?}"
    );
}

async fn domain_source(
    broker: &MockCluster<'static, DefaultProducerContext>,
    executor: Executor,
) -> KafkaSource {
    let (_rebalance, receiver) = watch::channel(1);
    let plan = KafkaSourcePlan {
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
        offset_mode: KafkaSourceOffsetMode::Domain {
            group_id: "metadata-worker-yield".to_string(),
            offsets: KafkaDomainOffsetHost::new(UnreachedOffsets),
            rebalance: receiver,
        },
    };
    KafkaSource::open(&plan, 0)
        .await
        .assured("the source accepts the test broker's configuration")
}

#[nervix_primitives::test]
async fn a_control_memory_refusal_preserves_its_typed_admission_cause() {
    let broker = cluster();
    let executor = Executor::default();
    let mut source = domain_source(&broker, executor.clone()).await;
    let capacity = executor.snapshot().commands_memory.capacity_bytes;
    let _held = executor
        .try_reserve(MemoryClass::Commands, capacity)
        .assured("the test occupies the command budget before resuming");
    let report = source
        .resume()
        .await
        .expect_err("a full command budget refuses the metadata job");
    assert!(matches!(
        report.downcast_ref::<KafkaSourceError>(),
        Some(KafkaSourceError::ControlAdmission)
    ));
    assert!(matches!(
        report.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted {
            class: "commands",
            ..
        })
    ));
    assert_eq!(executor.snapshot().connector_storage.admitted, 0);
}

#[nervix_primitives::test]
async fn control_admission_preserves_refusals_and_uses_the_client_deadline_while_queued() {
    let broker = cluster();
    let executor = Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            connector_storage: nervix_execution::ConnectorStorageWorkers::resolve(Some(
                std::num::NonZeroUsize::MIN,
            ))
            .assured("one worker fits the primitive permit bound"),
            pending_jobs: std::num::NonZeroUsize::MIN,
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only worker counts");
    let source = domain_source(&broker, executor.clone()).await;
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let occupied = executor.clone();
    let charge = executor
        .try_reserve(MemoryClass::Commands, 1)
        .assured("the first envelope fits");
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
        .assured("the first job holds the only connector worker");
    let charge = executor
        .try_reserve(MemoryClass::Commands, 1)
        .assured("the queued envelope fits");
    let mut queued = Box::pin(executor.run_storage(StorageClass::Connector, charge, |_, _| ()));
    let first_poll = poll_fn(|context| Poll::Ready(queued.as_mut().poll(context))).await;
    assert!(first_poll.is_pending());
    let result = source.control.run(&source.consumer, |_, _| Ok(())).await;
    drop(queued);
    // The queue has room again, but the only worker remains held. This wait must spend the
    // effective client deadline on admission without starting the native operation afterwards.
    let expired: Result<(), _> = source
        .control
        .run(&source.consumer, |_, _| {
            panic!("a control job whose admission deadline expired cannot start")
        })
        .await;
    release
        .send(())
        .assured("the test releases its worker before checking the refusal");
    running
        .await
        .assured("the supervisor is joined")
        .assured("the occupying job exits");

    let report = result.expect_err("a full connector queue refuses the next operation");
    assert!(matches!(
        report.current_context(),
        KafkaSourceError::ControlExecution
    ));
    assert!(matches!(
        report.downcast_ref::<ExecutionError>(),
        Some(ExecutionError::QueueFull {
            class: "connector_storage",
            pending: 1
        })
    ));
    let report = expired.expect_err("the configured deadline includes waiting for a worker");
    assert!(matches!(report.current_context(),
        KafkaSourceError::ControlTimeout { timeout } if *timeout == Duration::from_secs(1)));
    assert_eq!(executor.snapshot().commands_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn a_cancelled_control_call_finishes_before_the_next_can_mutate_its_consumer() {
    let broker = cluster();
    let executor = Executor::default();
    let source = domain_source(&broker, executor.clone()).await;
    let (started, has_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut first = Box::pin(source.control.run(&source.consumer, move |_, _| {
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

    let mut second = Box::pin(source.control.run(&source.consumer, |_, _| Ok(())));
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
