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
    let started = StdArc::new(AtomicUsize::new(0));
    let (release, release_rx) = watch::channel(false);
    transport_b
        .register_handler::<BlockingProgressRequest, _, _>({
            let started = StdArc::clone(&started);
            let release_rx = release_rx.clone();
            move |_context, _request| {
                let started = StdArc::clone(&started);
                let mut release_rx = release_rx.clone();
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    release_rx
                        .wait_for(|released| *released)
                        .await
                        .assured("the test retains its release sender until every request joins");
                    BlockingProgressResponse
                }
            }
        })
        .assured("the fresh test transport has no progress handler with this name");
    transport_b
        .register_handler::<LivenessRequest, _, _>(
            |_context, _request| async move { LivenessResponse },
        )
        .assured("the fresh test transport has no liveness handler with this name");

    let mut blocked = Vec::new();
    for _ in 0..connection::MANAGEMENT_PROGRESS_STREAMS {
        let requester = transport_a.clone();
        let target = node_b.clone();
        blocked.push(tokio::spawn(async move {
            requester.request(&target, BlockingProgressRequest).await
        }));
    }
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::task::consume_budget().await;
            if started.load(Ordering::Acquire) == connection::MANAGEMENT_PROGRESS_STREAMS {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .assured("the test requests start before their two-second request deadlines");

    let liveness = timeout(
        Duration::from_millis(250),
        transport_a.request(&node_b, LivenessRequest),
    )
    .await;

    release.send_replace(true);
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
    let liveness =
        liveness.verified("the liveness timeout result was checked by the assertion above");
    assert!(
        liveness.is_ok(),
        "liveness request should succeed: {liveness:?}"
    );
    assert_eq!(
        liveness.verified("the liveness response was checked by the assertion above"),
        LivenessResponse
    );
}
