//! An explicitly released response in the signed HTTPS peer.
//!
//! Layer: test harness.
//! - **Owns.** One response's arrival and release, including release before arrival.
//! - **Depends on.** Selected watch publication and cancellation.
//! - **Must not know.** Requests, signatures, licensing policy or cluster state.

use meticulous::ResultExt as _;
use nervix_primitives::sync::{CancellationToken, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Waiting,
    Held,
    Released,
}

#[derive(Clone, Debug)]
pub struct ResponseGate(watch::Sender<Phase>);

impl Default for ResponseGate {
    fn default() -> Self {
        Self(watch::channel(Phase::Waiting).0)
    }
}

impl ResponseGate {
    pub fn is_held(&self) -> bool {
        *self.0.borrow() == Phase::Held
    }

    pub fn release(&self) {
        self.0.send_replace(Phase::Released);
    }

    pub(crate) async fn hold(&self, stop: &CancellationToken) {
        let mut phase = self.0.subscribe();
        self.0.send_if_modified(|phase| {
            if *phase == Phase::Waiting {
                *phase = Phase::Held;
                true
            } else {
                false
            }
        });
        loop {
            nervix_primitives::task::consume_budget().await;
            if *phase.borrow_and_update() == Phase::Released {
                return;
            }
            nervix_primitives::select! {
                _ = stop.cancelled() => return,
                changed = phase.changed() => {
                    changed.assured("the held response retains the gate's publisher");
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;

    use super::*;

    #[nervix_primitives::test]
    async fn the_response_waits_until_release_or_peer_shutdown() {
        for release in [true, false] {
            let gate = ResponseGate::default();
            let stop = CancellationToken::new();
            let mut held = std::pin::pin!(gate.hold(&stop));
            assert!(futures_util::poll!(&mut held).is_pending());
            assert!(gate.is_held());
            if release {
                gate.release();
            } else {
                stop.cancel();
            }
            assert!(held.now_or_never().is_some());
        }
    }

    #[nervix_primitives::test]
    async fn releasing_before_arrival_cannot_rearm_the_gate() {
        let gate = ResponseGate::default();
        let stop = CancellationToken::new();
        gate.release();
        assert!(gate.hold(&stop).now_or_never().is_some());
        assert!(!gate.is_held());
    }
}
