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
use nervix_primitives::{
    publication::ArcSwap,
    sync::{StdArc, watch},
    time::Instant,
};

use super::PeerHealthStateSnapshot;

pub(super) struct PeerHealthStateWatcher<'a> {
    state: watch::Receiver<PeerHealthStateSnapshot>,
    observation_freshness: &'a ArcSwap<Duration>,
    /// The scheduling revision the caller last read the cluster state under: the one current when
    /// the watcher subscribed, and then the one each wait returned for. A caller subscribes before
    /// it reads, so a change it has not read yet always differs from this.
    observed: u64,
}

impl<'a> PeerHealthStateWatcher<'a> {
    pub(super) fn new(
        mut state: watch::Receiver<PeerHealthStateSnapshot>,
        observation_freshness: &'a ArcSwap<Duration>,
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
                .next_effective_transition(Instant::now(), **self.observation_freshness.load());
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

/// Installs the current interval under the parameter publisher's existing serialization.
pub(super) fn install_observation_freshness(
    freshness: &ArcSwap<Duration>,
    state: &watch::Sender<PeerHealthStateSnapshot>,
    duration: Duration,
) {
    if **freshness.load() == duration {
        return;
    }
    freshness.store(StdArc::new(duration));
    state.send_modify(|snapshot| {
        snapshot.advance_revision();
        snapshot.advance_scheduling_revision();
    });
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;
    use meticulous::OptionExt as _;
    use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};

    use super::*;
    use crate::cluster::{PeerHealthEndpoint, PeerHealthProbeOutcome, PeerHealthProbeResult};

    fn observed_health(freshness: Duration) -> PeerHealthStateSnapshot {
        let identity = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            ClusterNodeIncarnation::new(7),
        );
        let mut snapshot = PeerHealthStateSnapshot::default();
        let now = Instant::now();
        let target = snapshot
            .replace_endpoints(
                [PeerHealthEndpoint::new(
                    identity.clone(),
                    "node-2.example:7001"
                        .parse()
                        .assured("the test endpoint is valid"),
                )],
                now,
                freshness,
            )
            .into_iter()
            .next()
            .assured("one endpoint produces one target");
        snapshot.record_result(
            PeerHealthProbeResult::new(target, PeerHealthProbeOutcome::Healthy(identity), now),
            freshness,
        );
        snapshot
    }

    #[nervix_primitives::test]
    async fn installing_a_changed_timeout_wakes_an_already_prepared_wait() {
        let freshness = ArcSwap::from_pointee(Duration::from_secs(100));
        let (state, receiver) = watch::channel(PeerHealthStateSnapshot::default());
        let mut watcher = PeerHealthStateWatcher::new(receiver, &freshness);
        let mut waiting = std::pin::pin!(watcher.wait_for_change_or_next_transition());
        assert!(waiting.as_mut().now_or_never().is_none());
        install_observation_freshness(&freshness, &state, Duration::from_secs(20));
        assert!(
            waiting.as_mut().now_or_never().is_some(),
            "installing a changed timeout must wake a retained health wait"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_retained_watcher_uses_the_installed_timeout_for_its_next_deadline() {
        let duration = Duration::from_secs(100);
        let freshness = ArcSwap::from_pointee(duration);
        let (state, receiver) = watch::channel(observed_health(duration));
        let mut watcher = PeerHealthStateWatcher::new(receiver, &freshness);
        install_observation_freshness(&freshness, &state, Duration::from_secs(20));
        // A scheduling event wakes this retained subscription independently of policy publication.
        state.send_modify(|snapshot| snapshot.advance_scheduling_revision());
        watcher.wait_for_change_or_next_transition().await;
        let mut waiting = std::pin::pin!(watcher.wait_for_change_or_next_transition());
        assert!(waiting.as_mut().now_or_never().is_none());
        nervix_primitives::time::advance(Duration::from_secs(20)).await;
        assert!(
            waiting.as_mut().now_or_never().is_some(),
            "the next transition uses the installed timeout, rather than the subscribed value"
        );
    }

    #[nervix_primitives::test]
    async fn republishing_the_same_timeout_keeps_a_retained_wait_pending() {
        let duration = Duration::from_secs(100);
        let freshness = ArcSwap::from_pointee(duration);
        let (state, receiver) = watch::channel(PeerHealthStateSnapshot::default());
        let mut watcher = PeerHealthStateWatcher::new(receiver, &freshness);
        let mut waiting = std::pin::pin!(watcher.wait_for_change_or_next_transition());
        assert!(waiting.as_mut().now_or_never().is_none());
        install_observation_freshness(&freshness, &state, duration);
        assert!(waiting.as_mut().now_or_never().is_none());
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
