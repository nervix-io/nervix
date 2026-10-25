//! Exploration of the production signaling handoff at completion and cancellation.
//!
//! Layer: test harness.
//! - **Owns.** FIFO frame/header preservation on success and complete purge on failed signaling.
//! - **Depends on.** The production handoff, modeled channels and the canonical Shuttle runner.
//! - **Must not know.** Sockets, wall time, the registry or the source host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns handoff interleaving evidence")
)]

use std::time::Duration;

use error_stack::Report;
use nervix_connector::{NoIngestHeaders, RetainedIngestHeaders};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::sync::{Arc, oneshot};

use super::*;

fn timeout_failure() -> Report<WebsocketSignalingError> {
    Report::new(WebsocketSignalingError::Timeout {
        timeout: Duration::from_secs(1),
        unsatisfied: vec![".ready".to_string()],
    })
}

fn handoff_with_cancellation(capacity: usize) {
    shuttle::future::block_on(async move {
        let mut handoff = HandshakeFrames::new(capacity);
        let headers = Arc::new(RetainedIngestHeaders::capture(&NoIngestHeaders));
        let sink = handoff.sink(headers.clone());
        let (expire, mut expired) = oneshot::channel();
        let clock = nervix_primitives::task::spawn(async move {
            expire
                .send(())
                .assured("the handshake waits on its clock until it returns");
        });
        let first = WebsocketDataFrame::Text("first".to_string());
        let second = WebsocketDataFrame::Binary(vec![0, 255]);
        let deliver = async {
            sink.accept(first.clone()).await;
            sink.accept(second.clone()).await;
            Ok(())
        };
        let handshake = async {
            nervix_primitives::select! {
                biased;
                result = deliver => result,
                signal = &mut expired => {
                    signal.assured("the modeled clock sends its one expiry event");
                    Err(timeout_failure())
                }
            }
        };
        let result = handoff.complete(handshake).await;
        clock
            .await
            .assured("a panicking model task fails its execution");
        match result {
            Ok(mut handoff) => {
                assert_eq!(
                    capacity, 2,
                    "a single slot backpressures the second frame until expiry"
                );
                assert_eq!(handoff.len(), 2);
                for expected in [first, second] {
                    let message = handoff
                        .pop_front()
                        .assured("a completed handshake retains both frames");
                    assert_eq!(message.frame, expected);
                    assert!(Arc::ptr_eq(&message.headers, &headers));
                }
                assert!(handoff.is_empty());
            }
            Err(report) => {
                assert!(matches!(
                    report.current_context(),
                    WebsocketSignalingError::Timeout { .. }
                ));
                assert!(
                    sink.sender.is_closed(),
                    "failed signaling ends its exact queue"
                );
                let mut handoff = HandshakeFrames::new(capacity);
                let replacement = handoff.sink(headers.clone());
                replacement
                    .accept(WebsocketDataFrame::Text("replacement".to_string()))
                    .await;
                let message = handoff
                    .pop_front()
                    .assured("a replacement can use the released slot");
                assert_eq!(
                    message.frame,
                    WebsocketDataFrame::Text("replacement".to_string())
                );
            }
        }
    });
}

#[test]
fn shuttle_a_completed_or_cancelled_handshake_preserves_its_exact_frame_lifetime() {
    check_interleavings(|| handoff_with_cancellation(2));
}

#[test]
fn shuttle_a_full_handshake_handoff_backpressures_until_expiry_and_releases_every_slot() {
    check_interleavings(|| handoff_with_cancellation(1));
}

#[test]
fn shuttle_a_handshake_future_dropped_before_readiness_ends_its_exact_handoff() {
    check_interleavings(|| {
        shuttle::future::block_on(async {
            let mut handoff = HandshakeFrames::new(2);
            let headers = Arc::new(RetainedIngestHeaders::capture(&NoIngestHeaders));
            let sink = handoff.sink(headers);
            let (cancel, mut cancelled) = oneshot::channel();
            let controller = nervix_primitives::task::spawn(async move {
                cancel
                    .send(())
                    .assured("the cancellation observer lives until its controller joins");
            });
            let negotiating = async {
                sink.accept(WebsocketDataFrame::Text("pending".to_string()))
                    .await;
                std::future::pending::<error_stack::Result<(), WebsocketSignalingError>>().await
            };
            {
                let mut attempt = Box::pin(handoff.complete(negotiating));
                nervix_primitives::select! {
                    biased;
                    _ = &mut attempt => panic!("this handshake waits for its completion marker"),
                    signal = &mut cancelled => signal.assured("the controller supplies cancellation"),
                }
            }
            controller
                .await
                .assured("a panicking controller fails its model execution");
            assert!(
                sink.sender.is_closed(),
                "dropping the negotiating future ends its handoff before another connection can \
                 publish"
            );
        });
    });
}
