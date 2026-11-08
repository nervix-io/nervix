//! Layer: test harness.
//! - **Owns.** Empty, failed and staged-attempt metric witnesses beside delivery assertions.
//! - **Depends on.** The production emitter owner and its existing scripted connectors.
//! - **Must not know.** Metric storage or external driver implementations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests own metric witnesses")
)]

use super::*;

fn encoded_connector(
    script: impl Into<nervix_connector_fake::PublishScript>,
) -> EmitterConnectorState {
    let schema = crate::runtime::test_fixtures::EntrypointTestDomain {
        relays: &[],
        fields: &[("value", ParseAsType::I64)],
        branch_fields: &[],
    };
    EmitterConnectorState::opened(Box::new(
        crate::runtime::emitter_encoding::EncodedRecordEmitter {
            connector: Box::new(nervix_connector_fake::FakeEmitter::<
                nervix_connector_fake::Records,
            >::new(script)),
            codec: schema.codec(),
            batch: None,
        },
    ))
}

#[nervix_primitives::test]
async fn a_stop_drain_recovers_the_same_instance_and_finishes_before_close() {
    use nervix_connector::{EmitterConnector as _, ParsedRetryPolicy};
    use nervix_connector_fake::{
        EmitterLifecycleOperation as Op, FakeEmitter, PublishAnswer, Records,
    };

    let context = emitter_context();
    let plan = FakeEmitter::<Records>::new([PublishAnswer::Fail, PublishAnswer::ConfirmAll]);
    let observations = plan.observations();
    let driver = FakeEmitter::open(plan, context.emitter_host())
        .await
        .assured("fixture opens");
    let schema = crate::runtime::test_fixtures::EntrypointTestDomain {
        relays: &[],
        fields: &[("value", ParseAsType::I64)],
        branch_fields: &[],
    };
    let mut connector = EmitterConnectorState::opened(Box::new(
        crate::runtime::emitter_encoding::EncodedRecordEmitter {
            connector: Box::new(driver),
            codec: schema.codec(),
            batch: None,
        },
    ));
    let mut buffer = measured_buffer(&context);
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .assured("the fixture batch is buffered");
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::from_policy(ParsedRetryPolicy {
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    });
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let drained = connector
        .drain_for_stop("scripted", &context, &mut control, &mut buffer, deadline)
        .await;
    let EmitterStopDrainOutcome::Finished { report, .. } = drained else {
        panic!("the retained emitter must recover and drain by its original stop deadline");
    };
    assert_eq!(report.map(|report| report.messages), Some(1));
    assert!(buffer.is_empty());
    connector.close_transport(&context).await;
    let observed = observations.lock();
    assert_eq!(
        observed.lifecycle,
        [Op::Open, Op::Suspend, Op::Resume, Op::Finish, Op::Close]
    );
    assert_eq!(observed.finishes, [deadline]);
    assert_eq!(observed.payloads().len(), 2);
    assert_eq!(observed.payloads()[0], observed.payloads()[1]);
}

struct DestinationProbe<'a> {
    context: &'a EmitterConnectorContext,
}

fn measured_buffer(context: &EmitterConnectorContext) -> EmitterBatchBuffer {
    let mut buffer = EmitterBatchBuffer::new(
        context,
        &FlushPolicy::Immediate,
        Arc::new(EmitterBufferedMessages::default()),
    );
    buffer.set_flush_policy(RuntimeFlushPolicy::Each {
        interval: Duration::from_secs(60),
        max_batch_size: 1,
    });
    buffer
}

impl DestinationProbe<'_> {
    fn payload_sample(&self, family: &str, state: Option<&str>, expected: f64) {
        let text = self.context.runtime.inner.metrics.prometheus_text();
        let family = format!("{family}{{");
        let emitter = format!("emitter=\"{}\"", self.context.emitter);
        let mut observed = None;
        for line in text.lines() {
            if !line.starts_with(&family) || !line.contains(&emitter) {
                continue;
            }
            if let Some(state) = state
                && !line.contains(&format!("state=\"{state}\""))
            {
                continue;
            }
            let (_, value) = line
                .rsplit_once(' ')
                .assured("Prometheus samples end in a numeric value");
            observed = Some(
                value
                    .parse::<f64>()
                    .assured("the metric encoder emits numbers"),
            );
            break;
        }
        assert_eq!(
            observed,
            Some(expected),
            "wrong or absent payload sample in {text}"
        );
    }

    fn assert_sample(&self, family: &str, operation: &str, outcome: Option<&str>, expected: f64) {
        let text = self.context.runtime.inner.metrics.prometheus_text();
        let emitter = format!("emitter=\"{}\"", self.context.emitter);
        let operation = format!("operation=\"{operation}\"");
        let mut sample = None;
        for line in text.lines() {
            if !line.starts_with(family) || !line.contains(&emitter) || !line.contains(&operation) {
                continue;
            }
            if let Some(outcome) = outcome
                && !line.contains(&format!("outcome=\"{outcome}\""))
            {
                continue;
            }
            let (_, value) = line
                .rsplit_once(' ')
                .assured("Prometheus samples end in a value");
            sample = Some(
                value
                    .parse::<f64>()
                    .assured("the encoder emits numeric samples"),
            );
            break;
        }
        assert_eq!(sample, Some(expected), "missing or wrong sample in {text}");
    }

    fn attempts(&self, operation: &str, outcome: &str, expected: f64) {
        self.assert_sample(
            "nervix_emitter_publish_attempts_total{",
            operation,
            Some(outcome),
            expected,
        );
    }

    fn durations(&self, operation: &str, expected: f64) {
        self.assert_sample(
            "nervix_emitter_publish_duration_seconds_count{",
            operation,
            None,
            expected,
        );
    }
}

#[test]
fn destination_handles_register_every_attempt_child_before_work() {
    let context = emitter_context();
    let metrics = DestinationProbe { context: &context };
    for operation in ["publish", "commit"] {
        for outcome in ["succeeded", "transient", "misconfigured", "violation"] {
            metrics.attempts(operation, outcome, 0.0);
        }
        metrics.durations(operation, 0.0);
    }
}

#[nervix_primitives::test]
async fn an_empty_flush_is_not_a_destination_attempt() {
    let context = emitter_context();
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = EmitterConnectorState::opened(Box::new(unencodable_emitter()));
    let mut buffer = releasing_buffer();
    assert!(
        connector
            .flush_buffer(&context, &mut control, &mut buffer)
            .await
            .assured("an empty flush performs no encoding")
            .is_none()
    );
    DestinationProbe { context: &context }.durations("publish", 0.0);
}

#[nervix_primitives::test]
async fn a_failed_preparation_counts_once_and_retains_its_unresolved_batch() {
    let context = emitter_context();
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = EmitterConnectorState::opened(Box::new(unencodable_emitter()));
    let mut buffer = releasing_buffer();
    let batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let failure = match connector
        .publish_batch(&context, &mut control, &mut buffer, batch)
        .await
    {
        Ok(_) => panic!("the fixture's codec cannot encode its input schema"),
        Err(failure) => failure,
    };
    assert_eq!(
        *failure.error().current_context(),
        EmitterRuntimeError::EncodeBatch
    );
    assert_eq!(buffer.pending().len(), 1);
    let metrics = DestinationProbe { context: &context };
    metrics.attempts("publish", "violation", 1.0);
    metrics.attempts("publish", "transient", 0.0);
    metrics.durations("publish", 1.0);
    metrics.durations("commit", 0.0);
}

#[nervix_primitives::test]
async fn a_staged_write_and_its_commit_are_separate_destination_attempts() {
    let context = emitter_context();
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = EmitterConnectorState::opened(Box::new(staging_emitter(
        Duration::from_secs(60),
        u64::MAX,
        None,
    )));
    let mut buffer = measured_buffer(&context);
    let batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    assert!(
        publish(&mut connector, &context, &mut control, &mut buffer, batch)
            .await
            .is_none()
    );
    let metrics = DestinationProbe { context: &context };
    metrics.attempts("publish", "succeeded", 1.0);
    metrics.durations("publish", 1.0);
    metrics.durations("commit", 0.0);

    metrics.payload_sample("nervix_emitter_work_messages", Some("staged"), 1.0);

    let committed = connector
        .commit_staged_once(
            &context,
            &mut control,
            &mut buffer,
            EmitterCommitReason::Drain,
        )
        .await
        .assured("the scripted staged rows commit successfully");
    assert_eq!(committed.map(|report| report.messages), Some(1));
    metrics.payload_sample("nervix_emitter_work_messages", Some("staged"), 0.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("in_flight"), 0.0);
    metrics.attempts("commit", "succeeded", 1.0);
    metrics.durations("commit", 1.0);
    assert!(
        connector
            .commit_staged_once(
                &context,
                &mut control,
                &mut buffer,
                EmitterCommitReason::Drain
            )
            .await
            .assured("a commit with nothing staged is idle")
            .is_none()
    );
    metrics.durations("commit", 1.0);
}

#[nervix_primitives::test]
async fn a_failed_commit_retains_staged_rows_and_counts_its_successful_retry_separately() {
    let context = emitter_context();
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = EmitterConnectorState::opened(Box::new(staging_emitter(
        Duration::from_secs(60),
        u64::MAX,
        Some(EmitterWriteError::Commit {
            connector: "scripted",
        }),
    )));
    let mut buffer = measured_buffer(&context);
    let batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    assert!(
        publish(&mut connector, &context, &mut control, &mut buffer, batch)
            .await
            .is_none()
    );
    let failure = match connector
        .commit_staged_once(
            &context,
            &mut control,
            &mut buffer,
            EmitterCommitReason::Drain,
        )
        .await
    {
        Ok(_) => panic!("the scripted commit must report its transient failure"),
        Err(failure) => failure,
    };
    assert!(failure.current_context().is_retryable_publish_failure());
    assert_eq!(connector.staged_messages(), 1);
    let metrics = DestinationProbe { context: &context };
    metrics.attempts("commit", "transient", 1.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("staged"), 1.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("in_flight"), 0.0);
    metrics.attempts("commit", "succeeded", 0.0);
    metrics.durations("commit", 1.0);
    let committed = connector
        .commit_staged_once(
            &context,
            &mut control,
            &mut buffer,
            EmitterCommitReason::Drain,
        )
        .await
        .assured("retry commits the retained staged row");
    assert_eq!(committed.map(|report| report.messages), Some(1));
    assert_eq!(connector.staged_messages(), 0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("staged"), 0.0);
    metrics.attempts("commit", "transient", 1.0);
    metrics.attempts("commit", "succeeded", 1.0);
    metrics.durations("commit", 2.0);
    metrics.attempts("publish", "succeeded", 1.0);
}

#[nervix_primitives::test]
async fn unanswered_records_are_transient_and_their_retry_counts_one_success() {
    use nervix_connector_fake::{MemberAnswer, PublishAnswer};
    let context = emitter_context();
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = encoded_connector([
        PublishAnswer::Members(vec![MemberAnswer::Unanswered]),
        PublishAnswer::ConfirmAll,
    ]);
    let mut buffer = measured_buffer(&context);
    let batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let failure = match connector
        .publish_batch(&context, &mut control, &mut buffer, batch)
        .await
    {
        Ok(_) => panic!("the fake left its one record unanswered"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.error().current_context(),
        EmitterRuntimeError::UnansweredEmitterRecords {
            unanswered: 1,
            records: 1
        }
    ));
    assert!(
        failure
            .error()
            .current_context()
            .is_retryable_publish_failure()
    );
    assert_eq!(buffer.pending().len(), 1);
    let metrics = DestinationProbe { context: &context };
    metrics.attempts("publish", "transient", 1.0);
    metrics.attempts("publish", "violation", 0.0);
    metrics.durations("publish", 1.0);
    metrics.payload_sample("nervix_emitter_payloads_total", None, 1.0);
    metrics.payload_sample("nervix_emitter_payload_source_messages_total", None, 1.0);
    metrics.payload_sample("nervix_emitter_payload_encoded_bytes_total", None, 11.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("prepared"), 1.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("in_flight"), 0.0);
    let published = connector
        .flush_buffer(&context, &mut control, &mut buffer)
        .await
        .assured("the scripted retry confirms its retained record");
    assert_eq!(published.map(|report| report.messages), Some(1));
    assert!(buffer.is_empty());
    metrics.attempts("publish", "succeeded", 1.0);
    metrics.attempts("publish", "transient", 1.0);
    metrics.durations("publish", 2.0);
    metrics.payload_sample("nervix_emitter_payloads_total", None, 1.0);
    metrics.payload_sample("nervix_emitter_payload_source_messages_total", None, 1.0);
    metrics.payload_sample("nervix_emitter_payload_encoded_bytes_total", None, 11.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("buffered"), 0.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("prepared"), 0.0);
    metrics.payload_sample("nervix_emitter_work_messages", Some("in_flight"), 0.0);
}

#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn an_unknown_record_is_a_violation_without_accepting_a_foreign_answer() {
    let context = emitter_context();
    context
        .runtime
        .inner
        .fault_injection
        .answer_next_emitter_with_unknown_record(context.emitter.as_str());
    let faults = ConfiguredFaultInjection::default();
    let mut backoff = RuntimeReconnectBackoff::default();
    let (_shutdown, mut shutdown_rx) = watch::channel(false);
    let (_stop, mut stop_rx) = watch::channel(None);
    let mut control = EmitterPublishControl {
        fault_injection: &faults,
        shutdown_rx: &mut shutdown_rx,
        stop_rx: &mut stop_rx,
        backoff: &mut backoff,
    };
    let mut connector = encoded_connector([nervix_connector_fake::PublishAnswer::ConfirmAll]);
    let mut buffer = releasing_buffer();
    let batch = EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
    let failure = match connector
        .publish_batch(&context, &mut control, &mut buffer, batch)
        .await
    {
        Ok(_) => panic!("the host must refuse an answer for a record outside its write"),
        Err(failure) => failure,
    };
    assert_eq!(
        *failure.error().current_context(),
        EmitterRuntimeError::UnknownEmitterRecord {
            record: 1,
            records: 1
        }
    );
    assert!(
        !failure
            .error()
            .current_context()
            .is_retryable_publish_failure()
    );
    assert_eq!(buffer.pending().len(), 1);
    let metrics = DestinationProbe { context: &context };
    metrics.attempts("publish", "violation", 1.0);
    metrics.attempts("publish", "succeeded", 0.0);
    metrics.durations("publish", 1.0);
}
