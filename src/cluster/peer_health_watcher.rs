//! Event and deadline waiting over the current peer-health observations.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Waiting for a scheduling revision or the next effective health transition.
//! - **Depends on.** The parent health snapshot, governed watch channels and monotonic time.
//! - **Must not know.** Domain clocks, resource operations, graphs or consumer policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "gossip membership and management health coordinate topology outside record \
                  processing"
    )
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_primitives::{sync::watch, time::Instant};

use super::PeerHealthStateSnapshot;

pub(super) struct PeerHealthStateWatcher {
    state: watch::Receiver<PeerHealthStateSnapshot>,
    observation_freshness: Duration,
    /// The scheduling revision the caller last read the cluster state under: the one current when
    /// the watcher subscribed, and then the one each wait returned for. A caller subscribes before
    /// it reads, so a change it has not read yet always differs from this.
    observed: u64,
}

impl PeerHealthStateWatcher {
    pub(super) fn new(
        mut state: watch::Receiver<PeerHealthStateSnapshot>,
        observation_freshness: Duration,
    ) -> Self {
        let observed = state.borrow_and_update().scheduling_revision();
        Self {
            state,
            observation_freshness,
            observed,
        }
    }

    /// Waits until the scheduling revision differs from the one the caller last read under, or
    /// until a peer's health next takes effect. A change made after the caller read, even one made
    /// before this wait was prepared, ends the wait.
    pub(super) async fn wait_for_change_or_next_transition(&mut self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = self.state.borrow_and_update().scheduling_revision();
            if current != self.observed {
                self.observed = current;
                return;
            }
            let next_transition = self
                .state
                .borrow()
                .next_effective_transition(Instant::now(), self.observation_freshness);
            nervix_primitives::select! {
                changed = self.state.changed() => {
                    changed.assured(
                        "the cluster handle retains its peer-health state sender for its lifetime",
                    );
                }
                _ = async {
                    match next_transition {
                        Some(deadline) => nervix_primitives::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => return,
            }
        }
    }
}
