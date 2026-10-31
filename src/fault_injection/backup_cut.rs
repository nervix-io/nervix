//! Controlled backup cut boundaries for public backup scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped gates a backup cut reaches once it holds its lease, while the cluster
//!   applies its pause, once its domain is paused, and before its owners capture state, and how
//!   each gate holds every cut or node wait that reaches it.
//! - **Depends on.** Execution-sensitive watch channels, timers and cooperative budget, and
//!   vocabulary identities.
//! - **Must not know.** Archive contents, the nodes a cut must reach or backup recovery policy.
//!
//! A gate holds every cut that reaches it, not only the first. A backup whose outcome was not
//! recorded runs again under its execution reference and cuts each domain again, and the run whose
//! outcome a scenario observes must be held as the scenario declares. A timed hold counts from
//! each cut's own arrival and ends on the cut's own task, so neither the scenario runner nor the
//! step that timed it decides when a cut continues.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test backup cut gates and the holds they apply to each arriving cut"
    )
)]

use std::{collections::BTreeMap, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainName, DomainState, DomainStatus};
use nervix_primitives::{
    sync::{Arc, watch},
    time::Instant,
};

use super::FaultInjection;

/// Where a backup cut of a domain waits for a scenario to release it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum BackupCutBoundary {
    /// Once a quiesced cut holds its domain's lease, before it checks the nodes it must reach and
    /// before it pauses the domain.
    Lease,
    /// While the cluster applies a cut's pause: each node's wait for every live node to ready a
    /// runtime revision in which the domain is paused, the cut's own wait and every node's
    /// reconciliation alike. A hold here is a cluster slow to apply the pause, which the cut's
    /// deadline ends. A revision in which the domain runs again passes, so the resume is never
    /// held, and a newer revision ends a hold as it ends that wait.
    PauseApplication,
    /// Once a quiesced cut paused its domain and every live node applied the pause, before the
    /// drain's first poll. A hold here spends the cut's budget, as a slow application of the
    /// pause would.
    Drain,
    /// Before any owner captures state: a quiesced cut after its drain, and a stopped cut once it
    /// holds its lease.
    Capture,
}

/// The gate of one domain's backup cuts at one boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct BackupCutGateKey {
    domain: DomainName,
    boundary: BackupCutBoundary,
}

/// How a gate holds a cut that reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackupCutHold {
    /// Until the scenario releases the gate or times its holds.
    UntilReleased,
    /// For this long after the cut reached the gate.
    For(Duration),
    /// Not at all: the scenario released the gate.
    Released,
}

#[derive(Debug, Clone, Copy)]
struct BackupCutGateState {
    hold: BackupCutHold,
    /// When the first cut reached the gate.
    first_arrival: Option<Instant>,
}

/// Holds every backup cut of one domain that reaches one boundary, as its scenario declares.
#[derive(Debug)]
pub(super) struct BackupCutGate {
    state: watch::Sender<BackupCutGateState>,
}

impl Default for BackupCutGate {
    fn default() -> Self {
        let state = BackupCutGateState {
            hold: BackupCutHold::UntilReleased,
            first_arrival: None,
        };
        Self {
            state: watch::channel(state).0,
        }
    }
}

impl BackupCutGate {
    /// Holds a cut that reaches the gate now until the gate's hold lets it continue.
    async fn hold_arriving_cut(&self) {
        let arrived_at = Instant::now();
        // Subscribed before the arrival is published, so a scenario's answer to it is not missed.
        let mut held = self.state.subscribe();
        self.state.send_modify(|state| {
            if state.first_arrival.is_none() {
                state.first_arrival = Some(arrived_at);
            }
        });
        loop {
            nervix_primitives::task::consume_budget().await;
            let hold = held.borrow_and_update().hold;
            match hold {
                BackupCutHold::Released => return,
                BackupCutHold::UntilReleased => {
                    held.changed()
                        .await
                        .assured("the gate owns its state sender while a cut waits at it");
                }
                BackupCutHold::For(duration) => {
                    let Some(remaining) = duration.checked_sub(arrived_at.elapsed()) else {
                        return;
                    };
                    nervix_primitives::select! {
                        () = nervix_primitives::time::sleep(remaining) => return,
                        changed = held.changed() => {
                            changed.assured("the gate owns its state sender while a cut waits at it");
                        }
                    }
                }
            }
        }
    }

    async fn first_arrival(&self) -> Instant {
        let mut arrivals = self.state.subscribe();
        let state = *arrivals
            .wait_for(|state| state.first_arrival.is_some())
            .await
            .assured("the gate owns its state sender while a scenario waits for its first cut");
        state
            .first_arrival
            .verified("the wait ends once the first cut reached the gate")
    }

    fn hold_for(&self, duration: Duration) {
        self.state
            .send_modify(|state| state.hold = BackupCutHold::For(duration));
    }

    fn release(&self) {
        self.state
            .send_modify(|state| state.hold = BackupCutHold::Released);
    }
}

impl FaultInjection {
    /// Arms the gate of `domain`'s cuts at `boundary`: every cut that reaches it waits there until
    /// the scenario releases the gate or times its holds.
    pub fn pause_backup_cut_on(&self, domain: DomainName, boundary: BackupCutBoundary) {
        self.inner.backup_cut_gates.insert(
            BackupCutGateKey { domain, boundary },
            Arc::new(BackupCutGate::default()),
        );
    }

    /// Holds every cut that reaches the armed gate, one already waiting there included, for
    /// `duration` after its own arrival, and then lets it continue.
    pub fn hold_backup_cuts_for(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
        duration: Duration,
    ) {
        self.backup_cut_gate(domain, boundary).hold_for(duration);
    }

    /// Resolves once a cut reached the armed gate, with the instant the first one did.
    pub async fn wait_for_backup_cut_pause(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
    ) -> Instant {
        let gate = self.backup_cut_gate(domain, boundary);
        gate.first_arrival().await
    }

    /// Lets every cut waiting at the armed gate continue, and every later one pass it.
    pub fn release_backup_cut_pause(&self, domain: &DomainName, boundary: BackupCutBoundary) {
        self.backup_cut_gate(domain, boundary).release();
    }

    /// Releases every armed lease, pause-application, drain and capture gate before forgetting
    /// them. Cuts and waits that already borrowed a gate observe its release; those that arrive
    /// later find no gate to hold them. Returns how many gates cleanup released.
    pub fn release_all_backup_cut_pauses(&self) -> usize {
        let released = self.inner.backup_cut_gates.len();
        for gate in &self.inner.backup_cut_gates {
            gate.release();
        }
        self.inner.backup_cut_gates.clear();
        released
    }

    /// Holds a node's wait for every live node to ready a revision whose `domains` pause one with
    /// an armed pause-application gate, as that gate holds every wait that reaches it.
    pub(crate) async fn pause_backup_cut_application_if_armed(
        &self,
        domains: &BTreeMap<DomainName, DomainState>,
    ) {
        for (domain, state) in domains {
            nervix_primitives::task::consume_budget().await;
            if let DomainStatus::Paused = state.status {
                self.pause_backup_cut_if_armed(domain, BackupCutBoundary::PauseApplication)
                    .await;
            }
        }
    }

    pub(crate) async fn pause_backup_cut_if_armed(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
    ) {
        let key = BackupCutGateKey {
            domain: domain.clone(),
            boundary,
        };
        let Some(gate) = self
            .inner
            .backup_cut_gates
            .get(&key)
            .map(|gate| gate.value().clone())
        else {
            return;
        };
        gate.hold_arriving_cut().await;
    }

    fn backup_cut_gate(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
    ) -> Arc<BackupCutGate> {
        let key = BackupCutGateKey {
            domain: domain.clone(),
            boundary,
        };
        let Some(gate) = self.inner.backup_cut_gates.get(&key) else {
            panic!("the backup cut gate of domain '{domain}' at {boundary:?} is not armed");
        };
        gate.value().clone()
    }
}
