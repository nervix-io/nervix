//! A gap a scenario opens in the live view one node's acknowledgement watchers read.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The node runs a scenario makes one node's acknowledgement watchers miss, and whether
//!   a watcher has read each gap yet.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Gossip, the failure detector, acknowledgement records or the interconnect.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ClusterNodeIncarnation, ClusterNodeName};
use nervix_primitives::sync::watch;

use super::FaultInjection;

/// The node whose live view misses a run, and the node whose run it misses.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct LiveViewGap {
    observer: ClusterNodeName,
    missing: ClusterNodeName,
}

/// How far the observing node's acknowledgement watchers have read a gap a scenario opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveViewGapState {
    /// The next look still finds the run, so the watcher that makes it has seen the run live.
    Opening,
    /// Every look misses the run until the scenario closes the gap.
    Open,
    /// A watcher has missed the run at least once.
    Read,
    /// Every look observes this incarnation until the scenario closes the gap.
    Retained(ClusterNodeIncarnation),
}

impl FaultInjection {
    /// Makes `observer`'s acknowledgement watchers see `missing`'s run at their next look and miss
    /// it at every later one until the gap is closed, as a failure detector that briefly suspects
    /// a slow node drops a run the watchers already saw live.
    pub fn open_live_view_gap(&self, observer: ClusterNodeName, missing: ClusterNodeName) {
        let gap = LiveViewGap { observer, missing };
        let (state, _) = watch::channel(LiveViewGapState::Opening);
        self.inner.live_view_gaps.insert(gap, state);
    }

    /// Keeps one observer's ACK live view at the captured incarnation across a node restart.
    pub fn hold_live_view_incarnation(
        &self,
        observer: ClusterNodeName,
        missing: ClusterNodeName,
        incarnation: ClusterNodeIncarnation,
    ) {
        let (state, _) = watch::channel(LiveViewGapState::Retained(incarnation));
        self.inner
            .live_view_gaps
            .insert(LiveViewGap { observer, missing }, state);
    }

    /// Waits until an acknowledgement watcher of `observer` read the gap opened for `missing`.
    pub async fn wait_for_live_view_gap_read(
        &self,
        observer: &ClusterNodeName,
        missing: &ClusterNodeName,
    ) {
        let gap = LiveViewGap {
            observer: observer.clone(),
            missing: missing.clone(),
        };
        let mut state = {
            let open = self
                .inner
                .live_view_gaps
                .get(&gap)
                .assured("a scenario waits only for a gap it opened and has not closed");
            open.subscribe()
        };
        state
            .wait_for(|state| *state == LiveViewGapState::Read)
            .await
            .assured("the open gap keeps its sender until the scenario closes it");
    }

    /// Closes the gap opened for `missing` in `observer`'s live view: `missing`'s run is in that
    /// view again for every later read.
    pub fn close_live_view_gap(&self, observer: &ClusterNodeName, missing: &ClusterNodeName) {
        let gap = LiveViewGap {
            observer: observer.clone(),
            missing: missing.clone(),
        };
        self.inner.live_view_gaps.remove(&gap);
    }

    /// The scenario's override, if armed: an absent run or a retained incarnation.
    /// An opening gap first delegates one look to the actual live view.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "a scenario opens a gap only between one test observer and one missing node",
            key = "one test observer and missing node",
            bound = "one keyed read and a retained watch transition; no guard across await"
        )
    )]
    pub(crate) fn registrar_live_view_override(
        &self,
        observer: &ClusterNodeName,
        missing: &ClusterNodeName,
    ) -> Option<Option<ClusterNodeIncarnation>> {
        let gap = LiveViewGap {
            observer: observer.clone(),
            missing: missing.clone(),
        };
        let Some(state) = self.inner.live_view_gaps.get(&gap) else {
            return None;
        };
        let mut observed = Some(None);
        state.send_if_modified(|state| match state {
            LiveViewGapState::Opening => {
                *state = LiveViewGapState::Open;
                observed = None;
                false
            }
            LiveViewGapState::Open => {
                *state = LiveViewGapState::Read;
                true
            }
            LiveViewGapState::Read => false,
            LiveViewGapState::Retained(incarnation) => {
                observed = Some(Some(*incarnation));
                false
            }
        });
        observed
    }
}
