//! Holding a restarted voter's first relayed heartbeat while its leader decides whether to wait.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** A receiver-scoped heartbeat freeze and a one-shot scheduling observation barrier.
//! - **Depends on.** Typed node identities and the fault harness's existing pause primitive.
//! - **Must not know.** Schedules, runtime state, or the gossip failure detector's decisions.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName};
use nervix_primitives::sync::{Arc, atomic::Ordering, watch};

use super::{FaultInjection, TestPause};

#[derive(Debug)]
pub(super) struct StartupVoterGossip {
    voter: ClusterNodeName,
    first_heartbeat: watch::Sender<Option<CapturedHeartbeat>>,
    observation: Arc<TestPause>,
}

#[derive(Debug, Clone)]
struct CapturedHeartbeat {
    identity: ClusterNodeIdentity,
    heartbeat: u64,
}

impl FaultInjection {
    /// Arm only after every node has stopped, so each gossip process starts with no samples.
    pub fn hold_restarted_voter_heartbeat(
        &self,
        observer: ClusterNodeName,
        voter: ClusterNodeName,
    ) {
        self.inner.startup_voter_gossip.insert(
            observer,
            StartupVoterGossip {
                voter,
                first_heartbeat: watch::channel(None).0,
                observation: Arc::new(TestPause::default()),
            },
        );
    }

    pub(crate) fn startup_voter_gossip_exchange_is_blocked(
        &self,
        sender: &ClusterNodeName,
        receiver: &ClusterNodeName,
    ) -> bool {
        for (observer, peer) in [(sender, receiver), (receiver, sender)] {
            if let Some(fault) = self.inner.startup_voter_gossip.get(observer)
                && &fault.voter == peer
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn freeze_startup_voter_heartbeat(
        &self,
        observer: &ClusterNodeName,
        identity: ClusterNodeIdentity,
        heartbeat: u64,
    ) -> u64 {
        let Some(fault) = self.inner.startup_voter_gossip.get(observer) else {
            return heartbeat;
        };
        if identity.node_id() != &fault.voter {
            return heartbeat;
        }
        fault.first_heartbeat.send_if_modified(|captured| {
            if captured.is_some() {
                return false;
            }
            *captured = Some(CapturedHeartbeat {
                identity,
                heartbeat,
            });
            true
        });
        // The fixture restarts each voter once and never substitutes a different process.
        fault
            .first_heartbeat
            .borrow()
            .as_ref()
            .verified("the first matching heartbeat was installed above")
            .heartbeat
    }

    pub(crate) async fn wait_for_initial_voter_heartbeat_if_armed(
        &self,
        observer: &ClusterNodeName,
    ) {
        let mut heartbeat = {
            let Some(fault) = self.inner.startup_voter_gossip.get(observer) else {
                return;
            };
            fault.first_heartbeat.subscribe()
        };
        heartbeat
            .wait_for(Option::is_some)
            .await
            .assured("the armed restart fixture retains its heartbeat sender through observation");
    }

    pub(crate) async fn pause_startup_voter_observation_if_armed(
        &self,
        observer: &ClusterNodeName,
        dead_nodes: &BTreeSet<ClusterNodeName>,
    ) {
        let pause = {
            let Some(fault) = self.inner.startup_voter_gossip.get(observer) else {
                return;
            };
            let first_heartbeat = fault.first_heartbeat.borrow();
            let Some(captured) = first_heartbeat.as_ref() else {
                return;
            };
            if !dead_nodes.contains(captured.identity.node_id()) {
                return;
            }
            fault.observation.clone()
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
    }

    pub(crate) fn finish_startup_voter_observation(&self, observer: &ClusterNodeName) {
        if let Some(fault) = self.inner.startup_voter_gossip.get(observer)
            && fault.observation.claimed.load(Ordering::Acquire)
        {
            fault.observation.mark_delivered();
        }
    }

    pub async fn wait_for_startup_voter_observation(&self, observer: &ClusterNodeName) {
        let pause = self.startup_voter_observation(observer);
        pause.wait_until_reached().await;
    }

    pub async fn release_startup_voter_observation(&self, observer: &ClusterNodeName) {
        let pause = self.startup_voter_observation(observer);
        pause.release();
        pause.wait_until_delivered().await;
        self.inner.startup_voter_gossip.remove(observer);
    }

    fn startup_voter_observation(&self, observer: &ClusterNodeName) -> Arc<TestPause> {
        let fault = self
            .inner
            .startup_voter_gossip
            .get(observer)
            .verified("the restart fixture armed this observer before starting its nodes");
        fault.observation.clone()
    }
}
