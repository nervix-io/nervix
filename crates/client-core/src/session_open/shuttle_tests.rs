//! Bounds publication and cancellation before an ordinary request body is polled.
//!
//! Layer: test harness.
//! - **Owns.** The production body's once-only opening gate under publication and cancellation.
//! - **Depends on.** The real opening body, maintained tonic encoder and selected channels.
//! - **Must not know.** A duplicate encoder, HTTP transport or server execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns checks of the production opening boundary")
)]

use std::num::{NonZeroU64, NonZeroUsize};

use meticulous::OptionExt as _;
use nervix_client_wire::{
    ClientFrame, ClientMessage, ClientRequest, ProducerId, RequestId, SessionLimitSettings,
    SubmitBatchRequest,
};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

use super::*;

struct ObservedSource {
    frames: mpsc::Receiver<EncodedFrame<ClientFrame>>,
    polls: Arc<AtomicUsize>,
}
impl Stream for ObservedSource {
    type Item = EncodedFrame<ClientFrame>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.frames.poll_recv(context)
    }
}

type ObservedBody = OpeningBody<ClientFrame, ObservedSource>;

fn waiting_body() -> (
    ObservedBody,
    oneshot::Sender<SessionLimits>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let (sender, frames) = mpsc::channel(1);
    let request_id = RequestId::new(NonZeroU64::MIN);
    let frame = ClientMessage {
        request_id,
        request: ClientRequest::SubmitBatch(SubmitBatchRequest {
            producer: ProducerId::opened_by(request_id),
            batch: Bytes::from(vec![0x5a; 2048]),
        }),
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("the queued frame fits the explicit fixture policy");
    sender
        .try_send(frame)
        .assured("one frame fits the private empty channel");
    drop(sender);
    let (publication, limits) = oneshot::channel();
    let body = OpeningBody {
        state: BodyState::Waiting {
            limits,
            source: FrameSource {
                source: ObservedSource {
                    frames,
                    polls: polls.clone(),
                },
            },
        },
    };
    (body, publication, polls)
}

async fn assert_waiting(body: &mut ObservedBody, polls: &AtomicUsize) {
    poll_fn(|context| {
        assert!(http_body::Body::poll_frame(Pin::new(&mut *body), context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "ordinary frames stay unpolled before the announcement"
    );
}

#[test]
fn shuttle_announced_bounds_precede_polling_a_queued_native_frame() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let (mut body, publication, polls) = waiting_body();
            assert_waiting(&mut body, &polls).await;
            let defaults = SessionLimitSettings {
                frame_bytes: NonZeroUsize::new(SessionLimits::DEFAULT.frame_bytes())
                    .assured("positive default frame"),
                transfer_bytes: NonZeroUsize::new(SessionLimits::DEFAULT.transfer_bytes())
                    .assured("positive default transfer"),
                nesting_depth: NonZeroUsize::new(SessionLimits::DEFAULT.nesting_depth())
                    .assured("positive default depth"),
                collection_entries: NonZeroUsize::new(SessionLimits::DEFAULT.collection_entries())
                    .assured("positive default entries"),
                string_bytes: NonZeroUsize::new(SessionLimits::DEFAULT.string_bytes())
                    .assured("positive default string"),
            };
            let limits = SessionLimits::try_from(SessionLimitSettings {
                frame_bytes: NonZeroUsize::new(1024).assured("positive frame limit"),
                ..defaults
            })
            .assured("the actual peer's smaller frame limit is valid");
            let receiving = nervix_primitives::task::spawn(async move {
                let error =
                    poll_fn(|context| http_body::Body::poll_frame(Pin::new(&mut body), context))
                        .await
                        .assured("the queued request has a result")
                        .expect_err("tonic enforces the installed peer limit");
                assert_eq!(error.code(), Code::OutOfRange);
                assert!(polls.load(Ordering::SeqCst) > 0);
            });
            let announced = nervix_primitives::task::spawn(async move {
                publication.send(limits).assured("the opening body is live")
            });
            receiving.await.assured("the modeled receiver completed");
            announced.await.assured("the modeled publication completed");
        });
    });
}

#[test]
fn shuttle_an_ended_opening_polls_no_ordinary_native_frame() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let (mut body, publication, polls) = waiting_body();
            assert_waiting(&mut body, &polls).await;
            let receiving = nervix_primitives::task::spawn(async move {
                let error =
                    poll_fn(|context| http_body::Body::poll_frame(Pin::new(&mut body), context))
                        .await
                        .assured("the cancelled opening has a result")
                        .expect_err("ending the opening cancels its body");
                assert_eq!(error.code(), Code::Cancelled);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert!(http_body::Body::is_end_stream(&body));
            });
            let cancelled = nervix_primitives::task::spawn(async move { drop(publication) });
            receiving.await.assured("the modeled receiver completed");
            cancelled
                .await
                .assured("the modeled cancellation completed");
        });
    });
}
