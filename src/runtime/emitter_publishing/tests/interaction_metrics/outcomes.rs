//! Layer: test harness.
//! - **Owns.** Emitter outcome witnesses through production publishing and lifecycle owners.
//! - **Depends on.** The host, its public connector contract and the production fake connector.
//! - **Must not know.** External drivers, metric storage or copied publishing algorithms.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the production host")
)]

use nervix_connector::{EmitterFinishError, EmitterLifecycleError, EmitterResume};
use nervix_connector_fake::{FakeEmitter, PublishAnswer, Records};

use super::*;
use crate::runtime::emitter_retry::{EmitterRetryDeferral, EmitterRetrySchedule};

#[nervix_primitives::test]
async fn emitter_outcomes_count_every_typed_finish_and_its_exact_deadline() {
    let context = emitter_context();
    let mut driver = FakeEmitter::<Records>::new([]);
    driver.finish_results.extend([
        Ok(EmitterFinish::NothingHeld),
        Ok(EmitterFinish::Flushed),
        Err(Report::new(EmitterFinishError::Unsent {
            connector: "scripted",
            records: 3,
        })),
        Err(Report::new(EmitterFinishError::Flush {
            connector: "scripted",
        })),
    ]);
    let observations = driver.observations();
    let mut connector = encoded_driver(driver);
    let deadline = Instant::now();
    for expected_success in [true, true, false, false] {
        assert_eq!(
            connector.finish_transport(&context, deadline).await.is_ok(),
            expected_success
        );
    }
    let metrics = DestinationProbe { context: &context };
    for outcome in ["nothing_held", "flushed", "unsent", "failed"] {
        metrics.event(
            "nervix_emitter_finishes_total",
            &[("outcome", outcome)],
            1.0,
        );
    }
    metrics.event("nervix_emitter_unsent_records_total", &[], 3.0);
    assert_eq!(observations.lock().finishes, [deadline; 4]);
    let mut unavailable = EmitterConnectorState::Unavailable {
        reason: "not acquired".to_owned(),
        retry_open: false,
    };
    assert!(matches!(
        unavailable.finish_transport(&context, deadline).await,
        Ok(EmitterFinish::NothingHeld)
    ));
    metrics.event(
        "nervix_emitter_finishes_total",
        &[("outcome", "nothing_held")],
        2.0,
    );
}

#[nervix_primitives::test]
async fn emitter_outcomes_count_failed_and_recovered_acquisitions_on_the_same_instance() {
    let context = emitter_context();
    let mut driver = FakeEmitter::<Records>::new([]);
    driver.resume_results.extend([
        Err(Report::new(EmitterLifecycleError::Resume {
            connector: "scripted",
        })),
        Ok(EmitterResume::Ready),
    ]);
    let observations = driver.observations();
    let mut connector = encoded_driver(driver);
    let EmitterConnectorState::Open(open) = &mut connector else {
        panic!("fixture owns its driver");
    };
    open.lifecycle = crate::runtime::emitter_lifecycle::EmitterLifecycleState::new(false);
    assert!(connector.resume_transport(&context).await.is_err());
    connector
        .resume_transport(&context)
        .await
        .assured("retained driver recovers");
    connector
        .resume_transport(&context)
        .await
        .assured("ready transport requires no acquisition");
    let metrics = DestinationProbe { context: &context };
    metrics.event("nervix_emitter_opens_total", &[("outcome", "failed")], 1.0);
    metrics.event(
        "nervix_emitter_opens_total",
        &[("outcome", "succeeded")],
        1.0,
    );
    assert_eq!(
        observations.lock().lifecycle,
        [
            nervix_connector_fake::EmitterLifecycleOperation::Resume,
            nervix_connector_fake::EmitterLifecycleOperation::Resume,
        ]
    );
}

#[nervix_primitives::test]
async fn emitter_outcomes_count_a_cancelled_acquisition_without_dropping_the_driver() {
    let context = emitter_context();
    let mut driver = FakeEmitter::<Records>::new([]);
    driver.resume_gate = Some(Arc::new(nervix_primitives::sync::Notify::new()));
    let observations = driver.observations();
    let mut connector = encoded_driver(driver);
    let EmitterConnectorState::Open(open) = &mut connector else {
        panic!("fixture owns its driver");
    };
    open.lifecycle = crate::runtime::emitter_lifecycle::EmitterLifecycleState::new(false);
    let mut resume = Box::pin(connector.resume_transport(&context));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(resume.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(resume);
    assert!(matches!(connector, EmitterConnectorState::Open(_)));
    DestinationProbe { context: &context }.event(
        "nervix_emitter_opens_total",
        &[("outcome", "failed")],
        1.0,
    );
    assert_eq!(
        observations.lock().lifecycle,
        [nervix_connector_fake::EmitterLifecycleOperation::Resume]
    );
}

#[nervix_primitives::test]
async fn emitter_outcomes_retry_only_unresolved_members_and_leave_a_fresh_publish_unchanged() {
    let context = emitter_context();
    let driver = FakeEmitter::<Records>::new([
        PublishAnswer::DeliverFirstThenFail,
        PublishAnswer::ConfirmAll,
        PublishAnswer::ConfirmAll,
    ]);
    let observations = driver.observations();
    let mut connector = encoded_driver(driver);
    let mut buffer = measured_buffer(&context);
    for value in [1, 2, 3] {
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(value, value, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .assured("source batch is retained");
    }
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
    assert!(
        connector
            .flush_buffer(
                &context,
                &mut control,
                &mut buffer,
                EmitterPublishAttemptKind::Initial
            )
            .await
            .is_err()
    );
    assert_eq!(buffer.unresolved_messages(), 2);
    faults.fail_emitter_client_unavailable(context.emitter.as_str());
    assert!(
        connector
            .flush_buffer(
                &context,
                &mut control,
                &mut buffer,
                EmitterPublishAttemptKind::Retry,
            )
            .await
            .is_err()
    );
    DestinationProbe { context: &context }.event("nervix_emitter_records_retried_total", &[], 0.0);
    assert_eq!(observations.lock().payloads().len(), 1);
    faults.clear_emitter_client_fault(context.emitter.as_str());
    let report = connector
        .flush_buffer(
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishAttemptKind::Retry,
        )
        .await
        .assured("retry confirms the unresolved members");
    assert_eq!(report.map(|report| report.messages), Some(3));
    DestinationProbe { context: &context }.event("nervix_emitter_records_retried_total", &[], 2.0);
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(200)),
        )
        .assured("fresh batch is retained");
    connector
        .flush_buffer(
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishAttemptKind::Initial,
        )
        .await
        .assured("fresh publish succeeds");
    DestinationProbe { context: &context }.event("nervix_emitter_records_retried_total", &[], 2.0);
    let writes = observations.lock().payloads();
    assert_eq!(writes.iter().map(Vec::len).collect::<Vec<_>>(), [3, 2, 1]);
    assert_eq!(writes[1], writes[0][1..]);
}

#[nervix_primitives::test]
async fn emitter_outcomes_count_the_first_drain_attempt_of_an_already_pending_retry() {
    let context = emitter_context();
    let driver = FakeEmitter::<Records>::new([PublishAnswer::Fail, PublishAnswer::ConfirmAll]);
    let observations = driver.observations();
    let mut connector = encoded_driver(driver);
    let mut buffer = measured_buffer(&context);
    buffer
        .push(
            &context,
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
        )
        .assured("the source row is retained");
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
    let failure = connector
        .flush_buffer(
            &context,
            &mut control,
            &mut buffer,
            EmitterPublishAttemptKind::Initial,
        )
        .await
        .err()
        .assured("the first destination attempt fails");
    connector.reconnect_after(&failure, &context);
    let mut retry = EmitterRetrySchedule::default();
    retry.defer(
        &context,
        EmitterRetryDeferral {
            cause: EmitterRetryCause::Publish,
            wait: Duration::from_secs(300),
            acks: EmitterAcknowledgements::default(),
            waiting_for_stall_clear: false,
            reason: Some("destination refused the first attempt"),
        },
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let drained = connector
        .drain_for_stop(
            "scripted",
            &context,
            &mut control,
            &mut buffer,
            deadline,
            EmitterPublishAttemptKind::from_retry(retry.is_active()),
        )
        .await;
    let EmitterStopDrainOutcome::Finished { report, .. } = drained else {
        panic!("the drain retries immediately by its original deadline");
    };
    assert_eq!(report.map(|report| report.messages), Some(1));
    assert!(buffer.is_empty());
    let writes = observations.lock().payloads();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0], writes[1]);
    assert_eq!(observations.lock().finishes, [deadline]);
    let metrics = DestinationProbe { context: &context };
    metrics.event("nervix_emitter_records_retried_total", &[], 1.0);
    metrics.event("nervix_emitter_retries_total", &[("cause", "publish")], 1.0);
    metrics.event("nervix_emitter_retry_wait_seconds_total", &[], 300.0);
}

#[nervix_primitives::test]
async fn emitter_outcomes_classify_member_and_prepare_refusals_without_error_text() {
    let context = emitter_context();
    let at = Timestamp::from_unix_nanos(100);
    let mut batches = (0..5)
        .map(|_| EmitterPublishBatch::from_batch(input_batch(), at))
        .collect::<Vec<_>>();
    let position = |batch_index| EmitterRecordPosition {
        batch_index,
        row_index: 0,
    };
    let mut answers = PerRecordOutcome::empty();
    answers.reject(RejectedEmitterMember::external(
        position(0),
        at,
        "refused".to_owned(),
    ));
    answers.reject(RejectedEmitterMember::oversize(
        position(1),
        at,
        "refused".to_owned(),
    ));
    answers.reject(RejectedEmitterMember::invalid(
        position(2),
        at,
        "refused".to_owned(),
        [],
    ));
    RowAnswers::from(answers)
        .apply(&context, &mut batches, DeliveredAcknowledgements::Host)
        .await
        .assured("member errors follow message policy");
    let container = structured_message_error(
        at,
        MessageErrorCode::Validation,
        "refused".to_owned(),
        MessageErrorOperation::Encode,
        None,
        [],
    );
    finish_rejected_records(
        &context,
        &mut batches,
        vec![
            RejectedEmitterRecord::message(
                position(3),
                EmitterRejectionCode::Encode,
                "refused".to_owned(),
            ),
            RejectedEmitterRecord::structured(position(4), EmitterRejectionCode::Encode, container),
        ],
        MessageErrorOperation::Encode,
    )
    .await
    .assured("prepare refusals follow the same owner");
    let metrics = DestinationProbe { context: &context };
    for code in ["external", "oversize", "invalid"] {
        metrics.event(
            "nervix_emitter_records_rejected_total",
            &[("code", code)],
            1.0,
        );
    }
    metrics.event(
        "nervix_emitter_records_rejected_total",
        &[("code", "encode")],
        2.0,
    );
    metrics.event("nervix_emitter_records_failed_total", &[], 0.0);
    for batch in &batches {
        assert!(batch.resolved_rows().iter().all(|resolved| *resolved));
    }
    assert!(
        std::mem::size_of::<RejectedEmitterRecord>()
            <= std::mem::size_of::<StructuredMessageError>() + 4 * std::mem::size_of::<usize>()
    );
}

#[test]
fn emitter_outcomes_classify_confirmation_timeout_once_through_a_typed_report_chain() {
    let context = emitter_context();
    let failure = Report::new(nervix_connector::ConfirmationWindowError::AnswerTimedOut {
        timeout: Duration::from_secs(1),
    })
    .change_context(EmitterWriteError::Publish {
        connector: "scripted",
    })
    .change_context(EmitterRuntimeError::PublishBatch);
    context.record_confirmation_timeout(&failure);
    context.record_confirmation_timeout(&Report::new(EmitterRuntimeError::PublishBatch));
    DestinationProbe { context: &context }.event(
        "nervix_emitter_confirmation_timeouts_total",
        &[],
        1.0,
    );
}

#[test]
fn emitter_outcomes_record_only_successfully_scheduled_retry_delays() {
    let context = emitter_context();
    let mut schedule = EmitterRetrySchedule::default();
    for (cause, wait) in [
        (EmitterRetryCause::Publish, 125),
        (EmitterRetryCause::Connect, 250),
        (EmitterRetryCause::Commit, 500),
    ] {
        schedule.defer(
            &context,
            EmitterRetryDeferral {
                cause,
                wait: Duration::from_millis(wait),
                acks: EmitterAcknowledgements::default(),
                waiting_for_stall_clear: false,
                reason: Some("retry"),
            },
        );
    }
    schedule.defer(
        &context,
        EmitterRetryDeferral {
            cause: EmitterRetryCause::Publish,
            wait: Duration::MAX,
            acks: EmitterAcknowledgements::default(),
            waiting_for_stall_clear: false,
            reason: Some("outside the monotonic range"),
        },
    );
    let metrics = DestinationProbe { context: &context };
    for cause in ["publish", "connect", "commit"] {
        metrics.event("nervix_emitter_retries_total", &[("cause", cause)], 1.0);
    }
    metrics.event("nervix_emitter_retry_wait_seconds_total", &[], 0.875);
}
