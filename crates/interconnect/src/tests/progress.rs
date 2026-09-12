//! Isolation coverage for management progress and liveness stream capacity.
//!
//! Layer: test harness.
//!
//! - **Owns.** The saturation scenario for the progress stream subquota.
//! - **Depends on.** The interconnect test fixture and typed test requests.
//! - **Must not know.** Product runtime graphs or connector behavior.

use super::*;

#[tokio::test]
async fn progress_work_cannot_consume_liveness_streams() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let observation_deadline = Instant::now()
        .checked_add(LIVENESS_QUOTA_EVENT_FAILSAFE)
        .assured("the fixed liveness quota test failsafe fits in Tokio's instant range");
    let (progress_started, mut progress_started_rx) = watch::channel(0_usize);
    let (release, release_rx) = watch::channel(false);
    transport_b
        .register_handler::<BlockingProgressRequest, _, _>({
            let release_rx = release_rx.clone();
            let progress_started = progress_started.clone();
            move |_context, _request| {
                let mut release_rx = release_rx.clone();
                let progress_started = progress_started.clone();
                async move {
                    progress_started.send_modify(|started| {
                        *started = started
                            .checked_add(1)
                            .assured("the test starts only one bounded set of progress requests");
                    });
                    release_rx
                        .wait_for(|released| *released)
                        .await
                        .assured("the test retains its release sender until every request joins");
                    BlockingProgressResponse
                }
            }
        })
        .assured("the fresh test transport has no progress handler with this name");
    let (liveness_entered, mut liveness_entered_rx) = watch::channel(false);
    transport_b
        .register_handler::<LivenessRequest, _, _>({
            move |_context, _request| {
                let liveness_entered = liveness_entered.clone();
                async move {
                    liveness_entered.send_replace(true);
                    LivenessResponse
                }
            }
        })
        .assured("the fresh test transport has no liveness handler with this name");

    let mut blocked = Vec::new();
    for _ in 0..connection::stream_slots::MANAGEMENT_PROGRESS_STREAMS {
        let requester = transport_a.clone();
        let target = node_b.clone();
        blocked.push(tokio::spawn(async move {
            requester.request(&target, BlockingProgressRequest).await
        }));
    }
    timeout_at(
        observation_deadline,
        progress_started_rx
            .wait_for(|started| *started == connection::stream_slots::MANAGEMENT_PROGRESS_STREAMS),
    )
    .await
    .assured("every reserved progress stream enters its handler within the test failsafe")
    .assured("the registered progress handler retains its watch sender");

    let requester = transport_a.clone();
    let target = node_b.clone();
    let liveness = tokio::spawn(async move { requester.request(&target, LivenessRequest).await });
    timeout_at(
        observation_deadline,
        liveness_entered_rx.wait_for(|entered| *entered),
    )
    .await
    .assured(
        "the liveness handler enters while every progress handler remains blocked within the test \
         failsafe",
    )
    .assured("the registered liveness handler retains its watch sender");

    release.send_replace(true);
    let liveness = liveness
        .await
        .assured("the liveness request task contains no panic path");
    for request in blocked {
        let response = request
            .await
            .assured("the progress request task contains no panic path");
        assert!(
            response.is_ok(),
            "blocking progress request should finish after release: {response:?}"
        );
    }
    transport_a.shutdown().await;
    transport_b.shutdown().await;

    assert!(
        liveness.is_ok(),
        "liveness must retain a physical management stream: {liveness:?}"
    );
    assert_eq!(
        liveness.verified("the liveness response was checked by the assertion above"),
        LivenessResponse
    );
}

#[test]
fn subscription_interest_visibility_uses_shared_request_capacity() {
    assert_eq!(
        <SubscriptionInterestVisibilityRequest as InterconnectRequest>::SUBQUOTA,
        RequestSubquota::Shared,
        "a request that can remain open for gossip convergence must not reserve liveness capacity",
    );
}
