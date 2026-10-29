//! Interleavings of the production endpoint request window.
//!
//! Layer: test harness.
//! - **Owns.** Exact capacity, response ordering, deadline and intake-ending invariants.
//! - **Depends on.** The production window, endpoint binding lifetime and Shuttle runner.
//! - **Must not know.** HTTP sockets, wall-clock deadlines or publication memory ordering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks the production endpoint request window")
)]

use std::future::pending;

use meticulous::OptionExt as _;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{DomainName, DomainNodeRef, EndpointType, IngestorName, ModelKind};
use nervix_primitives::{
    sync::{Arc, oneshot},
    task::spawn,
};

use super::*;
use crate::runtime::{HttpRouteKey, RoutedEndpoint, endpoint_route_table::EndpointIntakeRoutes};

const JOIN: &str = "a panicking check task fails its Shuttle execution";

fn window(max: usize) -> Arc<EndpointRequestWindow> {
    Arc::new(EndpointRequestWindow {
        permits: Semaphore::new(max),
        timeout: Duration::from_secs(5),
        retry_after: Duration::from_secs(1),
    })
}

fn responses_release_their_own_slot_in_completion_order() {
    shuttle::future::block_on(async {
        let window = window(2);
        let (first_started, first_is_started) = oneshot::channel();
        let (second_started, second_is_started) = oneshot::channel();
        let (first_ack, first_is_acknowledged) = oneshot::channel();
        let (second_ack, second_is_acknowledged) = oneshot::channel();
        let first_window = window.clone();
        let first = spawn(async move {
            first_window
                .run_until(pending(), pending(), async {
                    first_started
                        .send(())
                        .assured("the observer waits for admission");
                    first_is_acknowledged
                        .await
                        .assured("the first ACK is explicitly released");
                    EndpointRequestOutcome::Completed
                })
                .await
        });
        let second_window = window.clone();
        let second = spawn(async move {
            second_window
                .run_until(pending(), pending(), async {
                    second_started
                        .send(())
                        .assured("the observer waits for admission");
                    second_is_acknowledged
                        .await
                        .assured("the second ACK is explicitly released");
                    EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::Rejected)
                })
                .await
        });
        first_is_started
            .await
            .assured("the first request enters its window");
        second_is_started
            .await
            .assured("the second request enters its window");
        assert_eq!(window.permits.available_permits(), 0);
        let full = window
            .run_until(pending(), pending(), async {
                panic!("a full window must not dispatch the refused payload")
            })
            .await;
        assert_eq!(
            full,
            EndpointRequestOutcome::NotAdmitted(ClientSubmissionRefusal::Busy)
        );
        second_ack.send(()).assured("the second request is held");
        assert_eq!(
            second.await.assured(JOIN),
            EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::Rejected)
        );
        assert_eq!(
            window.permits.available_permits(),
            1,
            "the first response remains held"
        );
        first_ack.send(()).assured("the first request remains held");
        assert_eq!(first.await.assured(JOIN), EndpointRequestOutcome::Completed);
        assert_eq!(window.permits.available_permits(), 2);
    });
}

#[test]
fn shuttle_endpoint_responses_release_their_own_slot_in_completion_order() {
    check_interleavings(responses_release_their_own_slot_in_completion_order);
}

fn ending_a_retained_intake_resolves_its_held_response_and_preserves_replacement() {
    shuttle::future::block_on(async {
        let routes = EndpointIntakeRoutes::default();
        let domain = DomainName::parse("endpoint_check").assured("a valid domain");
        let identity = DomainNodeRef::node_in(
            domain.clone(),
            ModelKind::Ingestor,
            IngestorName::parse("source").assured("a valid source"),
        );
        let key = HttpRouteKey {
            host: "edge.example.com".to_owned(),
            path: "/events".to_owned(),
        };
        routes.replace_domain(
            &domain,
            vec![(
                key.clone(),
                RoutedEndpoint {
                    endpoint_type: EndpointType::Http,
                    signaling_protocol: None,
                },
            )],
        );
        let binding = routes.bind(identity.clone(), (), std::slice::from_ref(&key));
        let retained = routes
            .resolve(&key.host, &key.path)
            .into_route()
            .assured("a published route");
        let window = window(1);
        let held_window = window.clone();
        let (started, is_started) = oneshot::channel();
        let response = spawn(async move {
            held_window
                .run_until(retained.bindings()[0].ended(), pending(), async {
                    started.send(()).assured("the observer waits for admission");
                    pending::<EndpointRequestOutcome>().await
                })
                .await
        });
        is_started
            .await
            .assured("the response is held by the exact source lifetime");
        let replacement = routes.bind(identity, (), std::slice::from_ref(&key));
        routes.unbind(&binding, std::slice::from_ref(&key));
        assert_eq!(
            response.await.assured(JOIN),
            EndpointRequestOutcome::Interrupted
        );
        assert_eq!(window.permits.available_permits(), 1);
        assert!(replacement.intake().is_some());
        let current = routes
            .resolve(&key.host, &key.path)
            .into_route()
            .assured("the replacement remains bound");
        assert_eq!(current.bindings().len(), 1);
    });
}

#[test]
fn shuttle_endpoint_ending_a_retained_intake_resolves_its_held_response_and_preserves_replacement()
{
    check_interleavings(
        ending_a_retained_intake_resolves_its_held_response_and_preserves_replacement,
    );
}

fn a_deadline_racing_an_ack_resolves_once_and_returns_its_slot() {
    shuttle::future::block_on(async {
        let window = window(1);
        let held_window = window.clone();
        let (started, is_started) = oneshot::channel();
        let (deadline, deadline_elapsed) = oneshot::channel();
        let (ack, acknowledged) = oneshot::channel();
        let response = spawn(async move {
            held_window
                .run_until(
                    pending(),
                    async {
                        let _ = deadline_elapsed.await;
                    },
                    async {
                        started.send(()).assured("the observer waits for admission");
                        let _ = acknowledged.await;
                        EndpointRequestOutcome::Completed
                    },
                )
                .await
        });
        is_started.await.assured("the response occupies the window");
        let expiring = spawn(async move {
            let _ = deadline.send(());
        });
        let completing = spawn(async move {
            let _ = ack.send(());
        });
        let outcome = response.await.assured(JOIN);
        assert!(matches!(
            outcome,
            EndpointRequestOutcome::Completed
                | EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut)
        ));
        expiring.await.assured(JOIN);
        completing.await.assured(JOIN);
        assert_eq!(window.permits.available_permits(), 1);
        assert_eq!(
            window
                .run_until(pending(), pending(), async {
                    EndpointRequestOutcome::Completed
                })
                .await,
            EndpointRequestOutcome::Completed
        );
        assert_eq!(window.permits.available_permits(), 1);
    });
}

#[test]
fn shuttle_endpoint_a_deadline_racing_an_ack_resolves_once_and_returns_its_slot() {
    check_interleavings(a_deadline_racing_an_ack_resolves_once_and_returns_its_slot);
}

fn intake_stop_wakes_a_held_response_without_waiting_for_source_close() {
    shuttle::future::block_on(async {
        use crate::runtime::{IngestorQuiesceCause, Runtime, test_ingestor_quiesce_control};
        let runtime = Runtime::default();
        let domain = DomainName::parse("endpoint_stop").assured("a valid domain");
        let ingestor = IngestorName::parse("source").assured("a valid ingestor");
        let control = test_ingestor_quiesce_control(
            &runtime,
            &domain,
            &ingestor,
            nervix_models::IngestQuiesceMode::Reject {
                retry_after: "1s".to_owned(),
            },
        );
        control.engage(IngestorQuiesceCause::EntityHold);
        {
            let mut paused = std::pin::pin!(control.wait_until_intake_stops());
            std::future::poll_fn(|context| {
                assert!(
                    paused.as_mut().poll(context).is_pending(),
                    "a resumable pause preserves the admitted root"
                );
                std::task::Poll::Ready(())
            })
            .await;
        }
        control.release(IngestorQuiesceCause::EntityHold);
        let window = window(1);
        let held_window = window.clone();
        let held_control = control.clone();
        let (started, is_started) = oneshot::channel();
        let response = spawn(async move {
            held_window
                .run_until(held_control.wait_until_intake_stops(), pending(), async {
                    started
                        .send(())
                        .assured("the observer waits for the held request");
                    pending::<EndpointRequestOutcome>().await
                })
                .await
        });
        is_started
            .await
            .assured("the request is held before intake stops");
        let stopping = spawn(async move {
            control.engage(IngestorQuiesceCause::Shutdown);
        });
        assert_eq!(
            response.await.assured(JOIN),
            EndpointRequestOutcome::Interrupted
        );
        stopping.await.assured(JOIN);
        assert_eq!(window.permits.available_permits(), 1);
    });
}

#[test]
fn shuttle_endpoint_intake_stop_wakes_a_held_response_without_waiting_for_ingestor_close() {
    check_interleavings(intake_stop_wakes_a_held_response_without_waiting_for_source_close);
}

fn every_binding_starts_before_a_shared_request_waits_for_acknowledgements() {
    shuttle::future::block_on(async {
        use crate::runtime::endpoint::{EndpointDispatchOutcome, dispatch_endpoint_bindings};
        let first_window = window(1);
        let second_window = window(1);
        let (first_started, first_is_started) = oneshot::channel();
        let (second_started, second_is_started) = oneshot::channel();
        let (first_ack, first_is_acknowledged) = oneshot::channel();
        let (second_ack, second_is_acknowledged) = oneshot::channel();
        let (first_done, first_is_done) = oneshot::channel();
        let (second_done, second_is_done) = oneshot::channel();
        let bindings = [
            (
                first_window.clone(),
                first_started,
                first_is_acknowledged,
                first_done,
            ),
            (
                second_window.clone(),
                second_started,
                second_is_acknowledged,
                second_done,
            ),
        ];
        let response = spawn(async move {
            dispatch_endpoint_bindings(bindings.into_iter().map(
                |(window, started, acknowledged, done)| async move {
                    let outcome = window
                        .run_until(pending(), pending(), async {
                            started
                                .send(())
                                .assured("the observer waits for both admissions");
                            acknowledged
                                .await
                                .assured("each consumer explicitly acknowledges");
                            EndpointRequestOutcome::Completed
                        })
                        .await;
                    done.send(())
                        .assured("the observer waits for each permit release");
                    EndpointDispatchOutcome::Ack(EndpointRequestReply {
                        outcome,
                        retry_after: window.retry_after,
                    })
                },
            ))
            .await
        });
        first_is_started
            .await
            .assured("the first binding receives the request");
        second_is_started
            .await
            .assured("the second binding receives the same request before either ACK");
        assert_eq!(first_window.permits.available_permits(), 0);
        assert_eq!(second_window.permits.available_permits(), 0);
        second_ack
            .send(())
            .assured("the second consumer finishes first");
        second_is_done
            .await
            .assured("the second window returns its slot");
        assert_eq!(first_window.permits.available_permits(), 0);
        assert_eq!(second_window.permits.available_permits(), 1);
        let mut response = std::pin::pin!(response);
        std::future::poll_fn(|context| {
            assert!(
                response.as_mut().poll(context).is_pending(),
                "the shared response still waits for the first ACK"
            );
            std::task::Poll::Ready(())
        })
        .await;
        first_ack
            .send(())
            .assured("the first consumer finishes last");
        first_is_done
            .await
            .assured("the first window returns its slot");
        assert_eq!(
            response.await.assured(JOIN),
            EndpointDispatchOutcome::Ack(EndpointRequestReply {
                outcome: EndpointRequestOutcome::Completed,
                retry_after: Duration::from_secs(1),
            })
        );
        assert_eq!(first_window.permits.available_permits(), 1);
        assert_eq!(second_window.permits.available_permits(), 1);
    });
}

#[test]
fn shuttle_endpoint_every_binding_starts_before_a_shared_request_waits_for_acknowledgements() {
    check_interleavings(every_binding_starts_before_a_shared_request_waits_for_acknowledgements);
}
