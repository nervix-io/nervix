//! Holding a restarted voter's first relayed heartbeat while its leader decides whether to wait,
//! and telling a restart fixture when the restarted cluster's leader stops waiting.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** A receiver-scoped heartbeat freeze, the fixture's reconciliation start gate, a
//!   one-shot scheduling observation barrier, and the end of a restarted leader's observation.
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
    reconciliation: Arc<TestPause>,
    observation: Arc<TestPause>,
}

#[derive(Debug, Clone)]
struct CapturedHeartbeat {
    identity: ClusterNodeIdentity,
    heartbeat: u64,
}

/// Whether a restart fixture waits for the restarted cluster's leader to end its startup voter
/// observation, and whether one has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupObservationEnd {
    /// No restart fixture waits for it.
    Unarmed,
    /// A fixture armed the wait after every node stopped, and no leader has ended it yet.
    Awaited,
    /// A leader of the restarted cluster passed its startup voter observation: it observed every
    /// voter live or its grace expired, so its automatic scheduling now decides.
    Ended,
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
                reconciliation: Arc::new(TestPause::default()),
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
        let (mut heartbeat, reconciliation) = {
            let Some(fault) = self.inner.startup_voter_gossip.get(observer) else {
                return;
            };
            (
                fault.first_heartbeat.subscribe(),
                fault.reconciliation.clone(),
            )
        };
        reconciliation.wait_until_released().await;
        heartbeat
            .wait_for(Option::is_some)
            .await
            .assured("the armed restart fixture retains its heartbeat sender through observation");
    }

    /// Starts the observer's reconciliation grace only after the fixture has established its
    /// leadership, while the other voters' startup and policy discovery remain free to progress.
    pub fn begin_startup_voter_observation(&self, observer: &ClusterNodeName) {
        self.inner
            .startup_voter_gossip
            .get(observer)
            .verified("the restart fixture armed this observer before starting its nodes")
            .reconciliation
            .release();
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

    /// Arm only after every node has stopped, so the end a fixture waits for belongs to a leader
    /// of the restarted cluster, not to a process that ran before the restart.
    pub fn await_startup_voter_observation_end(&self) {
        self.inner
            .startup_observation_end
            .send_replace(StartupObservationEnd::Awaited);
    }

    /// Records that this process, leading, has passed its startup voter observation. A pass reaches
    /// this only once no voter is left unobserved, and it may make an automatic scheduling decision
    /// right after.
    pub(crate) fn end_startup_voter_observation(&self) {
        self.inner.startup_observation_end.send_if_modified(|end| {
            let StartupObservationEnd::Awaited = *end else {
                return false;
            };
            *end = StartupObservationEnd::Ended;
            true
        });
    }

    /// Waits until a leader of the restarted cluster has passed its startup voter observation.
    pub async fn wait_for_startup_voter_observation_end(&self) {
        let mut end = self.inner.startup_observation_end.subscribe();
        end.wait_for(|end| *end == StartupObservationEnd::Ended)
            .await
            .assured("the fault state holds the observation sender for as long as a waiter does");
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
