//! RabbitMQ publication cost at the driver and production connector boundaries.
//!
//! Outside the layer order: a benchmark harness.
//! - **Owns.** An isolated broker, explicitly provisioned queue and bounded publication samples.
//! - **Depends on.** The public RabbitMQ emitter, connector contract, resolver and test environment.
//! - **Must not know.** Graph scheduling, relay state or the publisher's private implementation.
//!
//! Driver enqueue and driver round-trip controls differ only in the timed `basic.qos` answer.
//! They have different completion guarantees. The production sample includes record preparation
//! and outcome validation. Every sample drains and checks its exact message count outside timing,
//! keeping the broker queue bounded by one write. These are write costs, not graph latency.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "measures real RabbitMQ publication outside product execution"
    )
)]

use std::{
    fmt::Display,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, Throughput};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{BasicGetOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
};
use meticulous::OptionExt as _;
use nervix_connector::{
    BrokerPublishingMode, EmitterBoundedExecution, EmitterConnector as _, EmitterEventReporter,
    EmitterFinish, EmitterGeneralErrorHandler, EmitterHost, EmitterLifecycle,
    EmitterPayloadMeasurement, EmitterPayloadObserver, EmitterReadiness, EmitterRecord,
    EmitterRecordAcknowledgements, EmitterRecordId, EmitterResume, EmitterStagingDirectory,
    EmitterTransientErrorStatus, RecordEmitter,
};
use nervix_connector_rabbitmq::{RabbitMqQueueEmitter, RabbitMqQueueEmitterConfig};
use nervix_dns::{DnsConfiguration, DnsResolver};
use nervix_execution::Executor;
use nervix_models::{ClientConfigEntry, RabbitMqQueueName, Timestamp};
use nervix_primitives::{
    runtime::{Builder, Runtime},
    time::{Instant, timeout},
};
use nervix_test_environment::{
    ContainerMode, configure_process_lifecycle, restartable_rabbitmq::RestartableRabbitMq,
};
use tokio::process::Command;

#[path = "publish/memory.rs"]
mod memory;

const QUEUE: &str = "publication-benchmark";
const WAIT: Duration = Duration::from_secs(30);
const WRITE_COUNTS: [usize; 3] = [1, 100, 1_000];
const PAYLOAD_BYTES: [usize; 2] = [200, 4_096];

/// The benchmark's reporting boundary: failed setup, publication or cleanup invalidates a sample.
#[track_caller]
fn require<T, E: Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("RabbitMQ benchmark failed: {error:#}"),
    }
}

async fn reply<T>(operation: impl Future<Output = lapin::Result<T>>) -> T {
    let answered = require(timeout(WAIT, operation).await);
    require(answered)
}

/// No host service is reached by the record publisher. A future dependency must be measured
/// explicitly instead of silently disappearing into an inert benchmark implementation.
struct BenchmarkHost;

impl EmitterReadiness for BenchmarkHost {
    fn mark_ready(&self) {
        panic!("the benchmark drives the emitter lifecycle directly");
    }

    fn mark_unready(&self) {
        panic!("the benchmark drives the emitter lifecycle directly");
    }
}

impl EmitterTransientErrorStatus for BenchmarkHost {
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {
        panic!("a transient error invalidates the benchmark");
    }

    fn clear_transient_error(&self) {
        panic!("the RabbitMQ record publisher does not use host status");
    }
}

impl EmitterEventReporter for BenchmarkHost {
    fn report_error(&self, message: String) {
        panic!("RabbitMQ benchmark error: {message}");
    }
}

impl EmitterStagingDirectory for BenchmarkHost {
    fn staging_directory(&self) -> PathBuf {
        panic!("the RabbitMQ record publisher stages no files");
    }
}

impl EmitterGeneralErrorHandler for BenchmarkHost {
    fn handle_general_error(&self, _acks: &EmitterRecordAcknowledgements, reason: String) {
        panic!("RabbitMQ benchmark general error: {reason}");
    }
}

impl EmitterBoundedExecution for BenchmarkHost {
    fn executor(&self) -> Executor {
        panic!("the RabbitMQ record publisher submits no executor work");
    }
}

impl EmitterPayloadObserver for BenchmarkHost {
    fn record_prepared_payload(&self, _measurement: EmitterPayloadMeasurement) {
        panic!("the host supplies the RabbitMQ record publisher's prepared bytes");
    }

    fn record_payload_subdivisions(&self, _subdivisions: u64) {
        panic!("the host divides RabbitMQ payloads before publication");
    }
}

struct PublicationBenchmark<'runtime> {
    runtime: &'runtime Runtime,
    publication: Publication,
}

struct Publication {
    broker: RestartableRabbitMq,
    connection: Connection,
    driver: Channel,
    emitter: RabbitMqQueueEmitter,
}

#[derive(Clone, Copy)]
enum DriverCompletion {
    Enqueued,
    BrokerAnswered,
}

impl<'runtime> PublicationBenchmark<'runtime> {
    fn open(runtime: &'runtime Runtime) -> Self {
        let opened = runtime.block_on(async {
            let broker =
                require(RestartableRabbitMq::start("rabbitmq-publication-benchmark").await);
            let address = require(broker.address());
            let connection = reply(Connection::connect(
                address,
                ConnectionProperties::default(),
            ))
            .await;
            let driver = reply(connection.create_channel()).await;
            reply(driver.queue_declare(
                QUEUE.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            ))
            .await;
            let dns = require(DnsResolver::load(DnsConfiguration::system()).await);
            let mut emitter = require(
                RabbitMqQueueEmitter::open(
                    RabbitMqQueueEmitterConfig {
                        config: vec![
                            ClientConfigEntry {
                                key: "addr".into(),
                                value: address.into(),
                            },
                            ClientConfigEntry {
                                key: "timeout_ms".into(),
                                value: "30000".into(),
                            },
                            ClientConfigEntry {
                                key: "connect_timeout_ms".into(),
                                value: "30000".into(),
                            },
                        ],
                        dns,
                        queue: require(RabbitMqQueueName::try_from(QUEUE)),
                        mode: BrokerPublishingMode::NoAck,
                    },
                    EmitterHost::new(BenchmarkHost),
                )
                .await,
            );
            assert_eq!(
                require(require(timeout(WAIT, emitter.resume()).await)),
                EmitterResume::Ready
            );
            Publication {
                broker,
                connection,
                driver,
                emitter,
            }
        });
        Self {
            runtime,
            publication: opened,
        }
    }

    fn driver_write(
        &self,
        iterations: u64,
        count: usize,
        payload: &[u8],
        completion: DriverCompletion,
    ) -> Duration {
        self.runtime.block_on(async {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                nervix_primitives::task::consume_budget().await;
                let started = Instant::now();
                for _ in 0..count {
                    nervix_primitives::task::consume_budget().await;
                    let _not_requested = reply(self.publication.driver.basic_publish(
                        "".into(),
                        QUEUE.into(),
                        BasicPublishOptions::default(),
                        payload,
                        BasicProperties::default(),
                    ))
                    .await;
                }
                if let DriverCompletion::BrokerAnswered = completion {
                    reply(
                        self.publication
                            .driver
                            .basic_qos(0, BasicQosOptions::default()),
                    )
                    .await;
                }
                elapsed = elapsed
                    .checked_add(started.elapsed())
                    .assured("one bounded Criterion measurement fits a Duration");
                Self::drain(&self.publication.driver, count, payload).await;
            }
            elapsed
        })
    }

    fn production_write(&mut self, iterations: u64, count: usize, payload: &[u8]) -> Duration {
        self.runtime.block_on(async {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                nervix_primitives::task::consume_budget().await;
                let started = Instant::now();
                self.publication.publish(count, payload).await;
                elapsed = elapsed
                    .checked_add(started.elapsed())
                    .assured("one bounded Criterion measurement fits a Duration");
                Self::drain(&self.publication.driver, count, payload).await;
            }
            elapsed
        })
    }

    fn memory(&mut self, count: usize, payload: &[u8]) {
        self.production_write(1, count, payload);
        self.runtime.block_on(async {
            let before = memory::Snapshot::capture();
            self.publication.publish(count, payload).await;
            let after = memory::Snapshot::capture();
            after.report(&before, count, payload.len());
            Self::drain(&self.publication.driver, count, payload).await;
        });
    }

    async fn drain(channel: &Channel, count: usize, payload: &[u8]) {
        let expected = require(u32::try_from(count));
        // The production emitter and observer use different channels. Observe queue visibility
        // explicitly instead of assuming their broker actors have the same scheduling order.
        require(
            timeout(WAIT, async {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    let queue = reply(channel.queue_declare(
                        QUEUE.into(),
                        QueueDeclareOptions {
                            passive: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    ))
                    .await;
                    let observed = queue.message_count();
                    assert!(
                        observed <= expected,
                        "the queue contains more messages than this write"
                    );
                    if observed == expected {
                        break;
                    }
                    nervix_primitives::task::yield_now().await;
                }
                for remaining in (0..expected).rev() {
                    let Some(message) =
                        reply(channel.basic_get(QUEUE.into(), BasicGetOptions { no_ack: true }))
                            .await
                    else {
                        panic!("the broker did not return every published message");
                    };
                    assert_eq!(
                        message.data, payload,
                        "every message retains its exact payload"
                    );
                    assert_eq!(message.message_count, remaining);
                }
                assert!(
                    reply(channel.basic_get(QUEUE.into(), BasicGetOptions { no_ack: true },))
                        .await
                        .is_none(),
                    "the write leaves no additional message"
                );
            })
            .await,
        );
    }

    fn shutdown(mut self) {
        self.runtime.block_on(async {
            let deadline = Instant::now()
                .checked_add(WAIT)
                .assured("the benchmark's bounded shutdown fits a monotonic deadline");
            let finished = require(require(
                timeout(WAIT, self.publication.emitter.finish(deadline)).await,
            ));
            assert!(matches!(finished, EmitterFinish::NothingHeld));
            require(require(
                timeout(WAIT, self.publication.emitter.close()).await,
            ));
            reply(
                self.publication
                    .connection
                    .close(200, "benchmark completed".into()),
            )
            .await;
            drop(self.publication.emitter);
            require(require(
                timeout(WAIT, self.publication.broker.shutdown()).await,
            ));
        });
    }

    fn report_broker_diagnostics(&self) {
        let container = self.publication.broker.container_id();
        eprintln!("rabbitmq_broker_diagnostics container={container} begin");
        let result = self.runtime.block_on(async {
            timeout(
                WAIT,
                Command::new("docker")
                    .args(["logs", "--tail", "200", container])
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .kill_on_drop(true)
                    .status(),
            )
            .await
        });
        match result {
            Ok(Ok(status)) => eprintln!("rabbitmq_broker_diagnostics exit={status}"),
            Ok(Err(error)) => eprintln!("rabbitmq_broker_diagnostics failed: {error}"),
            Err(error) => eprintln!("rabbitmq_broker_diagnostics timed out: {error}"),
        }
    }
}

impl Publication {
    async fn publish(&mut self, count: usize, payload: &[u8]) {
        let mut records = Vec::with_capacity(count);
        for index in 0..count {
            records.push(EmitterRecord::new(
                EmitterRecordId::new(index),
                None,
                payload.to_vec(),
                Vec::new(),
                Timestamp::from_unix_nanos(0),
            ));
        }
        let outcome = require(timeout(WAIT, self.emitter.publish(records)).await).into_parts();
        if let Some(error) = outcome.infrastructure_error {
            panic!("RabbitMQ production publish failed: {error:#}");
        }
        assert!(
            outcome.rejected.is_empty(),
            "the provisioned queue accepts every record"
        );
        assert_eq!(outcome.delivered.len(), count);
        for (index, delivered) in outcome.delivered.iter().enumerate() {
            assert_eq!(delivered.index(), index);
        }
    }
}

fn verify_failure_cleanup(runtime: &Runtime) {
    let mut container = None;
    let result = catch_unwind(AssertUnwindSafe(|| {
        let fixture = PublicationBenchmark::open(runtime);
        container = Some(fixture.publication.broker.container_id().to_owned());
        panic!("RabbitMQ benchmark expected cleanup probe");
    }));
    assert!(
        result.is_err(),
        "the probe must unwind with its fixture alive"
    );
    let container = container.assured("the probe opened its isolated broker before unwinding");
    let output = require(require(runtime.block_on(async {
        timeout(
            WAIT,
            Command::new("docker")
                .args([
                    "ps",
                    "--all",
                    "--filter",
                    &format!("id={container}"),
                    "--format",
                    "{{.ID}}",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await
    })));
    assert!(
        output.status.success(),
        "the cleanup observer must reach Docker"
    );
    assert!(
        output.stdout.is_empty(),
        "unwinding must release the isolated broker"
    );
    eprintln!("rabbitmq_cleanup_evidence container={container} released=true");
}

fn measure(fixture: &mut PublicationBenchmark<'_>) {
    let mut criterion = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .configure_from_args();
    for payload_bytes in PAYLOAD_BYTES {
        let payload = vec![b'x'; payload_bytes];
        let mut group =
            criterion.benchmark_group(format!("rabbitmq_publish/{payload_bytes}_bytes"));
        for count in WRITE_COUNTS {
            fixture.memory(count, &payload);
            group.throughput(Throughput::Elements(require(u64::try_from(count))));
            group.bench_function(BenchmarkId::new("driver_enqueue", count), |bencher| {
                bencher.iter_custom(|iterations| {
                    fixture.driver_write(iterations, count, &payload, DriverCompletion::Enqueued)
                });
            });
            group.bench_function(BenchmarkId::new("driver_round_trip", count), |bencher| {
                bencher.iter_custom(|iterations| {
                    fixture.driver_write(
                        iterations,
                        count,
                        &payload,
                        DriverCompletion::BrokerAnswered,
                    )
                });
            });
            group.bench_function(BenchmarkId::new("production_no_ack", count), |bencher| {
                bencher.iter_custom(|iterations| {
                    fixture.production_write(iterations, count, &payload)
                });
            });
        }
        group.finish();
    }
    criterion.final_summary();
}

fn main() {
    configure_process_lifecycle(ContainerMode::Ephemeral);
    nervix_crypto::tls::install_default_provider();
    let runtime = require(
        Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build(),
    );
    // The fixture and its failure unwinding end before this entered context and runtime do.
    // Testcontainers' maintained Drop implementation needs the current runtime even in main.
    let _entered = runtime.enter();
    if std::env::args().any(|argument| argument == "--test") {
        verify_failure_cleanup(&runtime);
    }
    let mut fixture = PublicationBenchmark::open(&runtime);
    eprintln!(
        "rabbitmq_benchmark_fixture container={}",
        fixture.publication.broker.container_id()
    );
    let measured = catch_unwind(AssertUnwindSafe(|| measure(&mut fixture)));
    if let Err(payload) = measured {
        fixture.report_broker_diagnostics();
        resume_unwind(payload);
    }
    fixture.shutdown();
}
