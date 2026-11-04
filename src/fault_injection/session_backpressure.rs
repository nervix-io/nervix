//! Observing an actual session request that waits for room to answer its client.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** One node-scoped observation of the first request acceptance future to wait.
//! - **Depends on.** Node and request identities, the shared injector and watch notifications.
//! - **Must not know.** How a session queues replies or how its transport supplies capacity.
//!
//! The observer polls the production future unchanged. It supplies neither capacity nor a pause
//! to that future; only the harness waiting for the observation receives a notification.

use std::{future::poll_fn, pin::pin};

use nervix_client_wire::RequestId;
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::watch;

use super::FaultInjection;

#[derive(Clone, Debug)]
pub(super) struct ArmedSessionBackpressure {
    node: ClusterNodeName,
    observer: SessionAcceptanceObserver,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionAcceptanceObserver {
    waiting: watch::Sender<Option<RequestId>>,
}

impl SessionAcceptanceObserver {
    pub(crate) async fn observe<F: Future>(
        &self,
        request_id: Option<RequestId>,
        has_no_room: impl Fn() -> bool,
        acceptance: F,
    ) -> F::Output {
        let mut acceptance = pin!(acceptance);
        poll_fn(|context| {
            let polled = acceptance.as_mut().poll(context);
            if polled.is_pending()
                && has_no_room()
                && let Some(request_id) = request_id
            {
                self.waiting.send_if_modified(|waiting| {
                    if waiting.is_some() {
                        return false;
                    }
                    *waiting = Some(request_id);
                    true
                });
            }
            polled
        })
        .await
    }
}

impl FaultInjection {
    /// Observe the next session opened on `node`. Existing sessions retain their own observer.
    /// No session is paused and no response is read on the client's behalf.
    pub fn observe_session_backpressure_on(
        &self,
        node: ClusterNodeName,
    ) -> watch::Receiver<Option<RequestId>> {
        let (waiting, observation) = watch::channel(None);
        self.inner
            .session_backpressure
            .send_replace(Some(ArmedSessionBackpressure {
                node,
                observer: SessionAcceptanceObserver { waiting },
            }));
        observation
    }

    pub(crate) fn session_acceptance_observer_on(
        &self,
        node: &ClusterNodeName,
    ) -> Option<SessionAcceptanceObserver> {
        let armed = self.inner.session_backpressure.borrow();
        let armed = armed.as_ref()?;
        if &armed.node != node {
            return None;
        }
        Some(armed.observer.clone())
    }
}
