//! Progress reports for the requests a session still serves.
//!
//! Layer: edges.
//!
//! - **Owns.** Sending every in-flight request that asked for progress a `RequestProgress` reply
//!   at its interval, from when the session takes the request until it queues its terminal reply.
//! - **Depends on.** The session's in-flight requests and its outbound queue.
//! - **Must not know.** What a request does, how long it takes or which lane serves it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_client_wire::RequestProgress;
use nervix_primitives::{
    sync::Arc,
    time::{Instant, sleep_until},
};

use super::SessionShared;

/// Reports every in-flight request that asked for progress at its interval until the session
/// ends. A report that the session queues just before a request's terminal reply can reach the
/// client after that reply, which the protocol tells the client to ignore.
pub(super) async fn run_request_progress(shared: Arc<SessionShared>) {
    loop {
        nervix_primitives::task::consume_budget().await;
        // Registering for the next registration before reading the schedules makes a request
        // registered after the read wake this loop, so its first report is never missed.
        let changed = shared.progress_changed.notified();
        let mut changed = std::pin::pin!(changed);
        changed.as_mut().enable();
        let due = shared.take_due_progress(Instant::now());
        for request_id in due.requests {
            nervix_primitives::task::consume_budget().await;
            let frame = RequestProgress { request_id }
                .encode(shared.limits())
                .assured("a progress report is far smaller than the smallest session frame limit");
            if shared.send_frame(frame).await.is_err() {
                return;
            }
        }
        let next_due = async {
            match due.next {
                Some(next) => sleep_until(next).await,
                None => std::future::pending::<()>().await,
            }
        };
        nervix_primitives::select! {
            () = shared.ended() => return,
            () = changed => {},
            () = next_due => {},
        }
    }
}
