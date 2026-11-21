//! Interleavings of resume and a client's departure from the endpoint pause buffer.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that expiry and disconnect cannot replay an unadmitted request.
//! - **Depends on.** The production pause buffer, response owner and Shuttle runner.
//! - **Must not know.** Listener sockets or elapsed wall time.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks the endpoint pause queue and its waiting response"
    )
)]

use meticulous::OptionExt as _;
use nervix_connector::{IngestMetadataRow, NoIngestHeaders, NoIngestMetadata};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{
    DomainName, EndpointPauseResponse, HttpIngestorEntity, IngestQuiesceMode, IngestorEntityKind,
    IngestorName, Timestamp, Unit,
};
use nervix_primitives::task::spawn;

use super::*;
use crate::{
    metrics::RuntimeMetrics,
    runtime::ingestor_quiesce::{EndpointPauseIntake, IngestorQuiesceCause},
};

#[derive(Clone, Copy)]
enum ClientEnding {
    Deadline,
    Disconnect,
}

fn pause_control() -> Arc<IngestorQuiesceControl> {
    let metrics = RuntimeMetrics::default();
    let labels = metrics.register_ingestor_quiesce(
        &DomainName::parse("endpoint_check").assured("a valid domain"),
        &IngestorName::parse("incoming").assured("a valid ingestor"),
        Unit::SINGLE,
        None,
        IngestorEntityKind::Http(HttpIngestorEntity::Endpoint),
    );
    let control = IngestorQuiesceControl::new(
        IngestQuiesceMode::EndpointBuffer {
            max_size: "1KiB".to_owned(),
            response: EndpointPauseResponse::AfterResume,
        },
        metrics,
        labels,
    );
    control.engage(IngestorQuiesceCause::EntityHold);
    Arc::new(control)
}

fn ending_races_resume(ending: ClientEnding) {
    shuttle::future::block_on(async move {
        let control = pause_control();
        let (buffered, is_buffered) = oneshot::channel();
        let (finish, client_ends) = oneshot::channel();
        let client_control = control.clone();
        let client = spawn(async move {
            let metadata = IngestMetadataRow::headers(&NoIngestHeaders, &NoIngestMetadata(4));
            let EndpointPauseIntake::Waiting(wait) = client_control.pause_endpoint(
                b"held",
                metadata,
                Timestamp::from_unix_nanos(1),
                None,
            ) else {
                panic!("a body within capacity must wait behind the engaged hold");
            };
            buffered
                .send(())
                .assured("the replay task waits for the body");
            let shutdown = CancellationToken::new();
            let end = async {
                client_ends
                    .await
                    .assured("the harness ends the client wait");
            };
            match ending {
                ClientEnding::Deadline => Some(wait.finish_until(&shutdown, end).await),
                ClientEnding::Disconnect => nervix_primitives::select! {
                    biased;
                    reply = wait.finish_until(&shutdown, pending()) => Some(reply),
                    () = end => None,
                },
            }
        });
        is_buffered
            .await
            .assured("the client placed its body in the queue");
        let replay_control = control.clone();
        let replay = spawn(async move {
            replay_control.release(IngestorQuiesceCause::EntityHold);
            let Some(retained) = replay_control.take_buffered() else {
                return false;
            };
            let reply = retained
                .reply()
                .assured("the retained body has a waiting client");
            if !reply.admit() {
                retained.refuse_endpoint();
                return false;
            }
            let mut delivery = retained.into_endpoint(&replay_control);
            assert_eq!(delivery.payload.payload(), b"held");
            delivery
                .reply
                .take()
                .assured("the admitted body owns the reply")
                .answer(EndpointPauseOutcome::Finished(
                    EndpointRequestOutcome::Completed,
                ));
            true
        });
        // The replay may already have completed the session before the client event is sent.
        let _ = finish.send(());
        let outcome = client.await.assured("the client path must terminate");
        let admitted = replay.await.assured("the replay path must terminate");
        match outcome {
            Some(EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::PauseExpired)) => {
                assert!(!admitted);
            }
            Some(
                EndpointRequestOutcome::Completed
                | EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut),
            ) => {
                assert!(admitted);
            }
            None => assert!(matches!(ending, ClientEnding::Disconnect)),
            other => panic!("unexpected outcome while racing a pause ending: {other:?}"),
        }
        assert_eq!(control.counters().buffered_records, 0);
        assert_eq!(control.counters().buffered_bytes, 0);
        assert_eq!(control.counters().dropped_total, u64::from(!admitted));
        assert!(control.take_buffered().is_none());
    });
}

#[test]
fn shuttle_endpoint_pause_deadline_and_resume_resolve_the_body_once() {
    check_interleavings(|| ending_races_resume(ClientEnding::Deadline));
}

#[test]
fn shuttle_endpoint_pause_disconnect_and_resume_resolve_the_body_once() {
    check_interleavings(|| ending_races_resume(ClientEnding::Disconnect));
}

fn disconnect_races_returning_the_body() {
    shuttle::future::block_on(async {
        let control = pause_control();
        let (buffered, is_buffered) = oneshot::channel();
        let (disconnect, disconnected) = oneshot::channel();
        let client_control = control.clone();
        let client = spawn(async move {
            let EndpointPauseIntake::Waiting(wait) = client_control.pause_endpoint(
                b"held",
                IngestMetadataRow::headers(&NoIngestHeaders, &NoIngestMetadata(4)),
                Timestamp::from_unix_nanos(1),
                None,
            ) else {
                panic!("the buffer has room for the request");
            };
            buffered.send(()).assured("the model waits for retention");
            disconnected.await.assured("the model ends the client wait");
            drop(wait);
        });
        is_buffered.await.assured("the client has retained a body");
        control.release(IngestorQuiesceCause::EntityHold);
        let (taken, is_taken) = oneshot::channel();
        let (interrupt, interrupted) = oneshot::channel();
        let replay_control = control.clone();
        let replay = spawn(async move {
            let retained = replay_control
                .take_buffered()
                .assured("replay takes the retained body after resume");
            replay_control.engage(IngestorQuiesceCause::EntityHold);
            taken
                .send(())
                .assured("the model waits for the second hold");
            interrupted.await.assured("the model interrupts replay");
            drop(retained);
        });
        is_taken
            .await
            .assured("the body is out while the ingestor pauses again");
        disconnect.send(()).assured("the client is still waiting");
        interrupt.send(()).assured("replay is held on interruption");
        client.await.assured("the client path terminates");
        replay.await.assured("the replay path terminates");
        assert_eq!(control.counters().buffered_records, 0);
        assert_eq!(control.counters().dropped_total, 1);
        control.release(IngestorQuiesceCause::EntityHold);
        assert!(control.take_buffered().is_none());
    });
}

#[test]
fn shuttle_endpoint_pause_disconnect_removes_the_body_when_replay_is_interrupted() {
    check_interleavings(disconnect_races_returning_the_body);
}
