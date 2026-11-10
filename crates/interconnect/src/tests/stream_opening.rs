//! Typed stream-opening refusal checks.
//!
//! Layer: test harness.
//! Owns: logical-operation outcomes across the authenticated stream-opening boundary.
//! Depends on: public transports, their bounded fixtures and typed outcomes.
//! Must not know: runtime graphs or snapshot contents.

use super::*;

#[nervix_primitives::test]
async fn not_ready_stream_opening_keeps_the_remote_operation_outcome() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>(|_context, _request| async {
            Err::<StreamingResponse, _>(StreamHandlerError::with_cause(Report::new(
                RemoteOperationFailure::not_ready(RemoteOperationSubject::domain(
                    &"stream_state".parse().expect("the domain name is valid"),
                )),
            )))
        })
        .expect("resource stream handler should register");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");

    let Err(error) = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
    else {
        panic!("an unready operation must refuse before returning a stream");
    };
    assert!(matches!(
        error.downcast_ref::<TransportError>(),
        Some(TransportError::RemoteRejected { status: 425, .. })
    ));
    assert_eq!(
        error.downcast_ref::<RemoteOperationFailureKind>(),
        Some(&RemoteOperationFailureKind::NotReady),
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn stream_opening_distinguishes_every_operation_refusal_from_transport_failure() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let subject = RemoteOperationSubject::domain(&"stream_state".parse().expect("valid domain"));
    let (selected, current) = watch::channel(RemoteOperationFailure::rejected(subject.clone()));
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>(move |_context, _request| {
            let failure = current.borrow().clone();
            async move {
                Err::<StreamingResponse, _>(StreamHandlerError::with_cause(Report::new(failure)))
            }
        })
        .expect("the operation-refusal handler registers");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the authenticated target becomes ready");
    for failure in [
        RemoteOperationFailure::rejected(subject.clone()),
        RemoteOperationFailure::unavailable(subject.clone()),
        RemoteOperationFailure::failed(subject, "the snapshot reader failed"),
    ] {
        let kind = RemoteOperationFailureKind::from(&failure);
        let status = match kind {
            RemoteOperationFailureKind::Rejected => 409,
            RemoteOperationFailureKind::Unavailable => 404,
            RemoteOperationFailureKind::Failed => 500,
            RemoteOperationFailureKind::NotReady => unreachable!("separately covered"),
        };
        selected.send_replace(failure);
        let Err(error) = transport_a
            .request_stream(&node_b, ResourceStreamRequest)
            .await
        else {
            panic!("a refused operation cannot return a stream");
        };
        assert_eq!(
            error.downcast_ref::<RemoteOperationFailureKind>(),
            Some(&kind)
        );
        let Some(TransportError::RemoteRejected {
            status: actual,
            message,
        }) = error.downcast_ref::<TransportError>()
        else {
            panic!("the refusal retains its transport context: {error:#}");
        };
        assert_eq!(*actual, status);
        if kind == RemoteOperationFailureKind::Failed {
            assert!(message.contains("the snapshot reader failed"));
        }
    }
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
