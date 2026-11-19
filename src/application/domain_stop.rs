//! A domain's stop on the control plane: the leader's owner of every stopping domain, the `STOP`
//! statement that requests a stop, and the node-to-node requests of its drain and report.
//!
//! Layer: control plane.
//! - **Owns.** Draining a stopping domain until its work in motion rests or a request's clause
//!   acts, ending or calling off its stop in consensus, collecting what its teardown cut, the
//!   answer each request gets, and the status of a stopping domain.
//! - **Depends on.** Committed domain records and stop requests, committed licensing time, the
//!   runtime's stop view and report, and the interconnect.
//! - **Must not know.** How a node classifies or tears down its work, or how the session protocol
//!   presents an answer.
//!
//! One leader task owns each stopping domain, resumed by the next leader from the committed record:
//! a stop's requests, the instant each was made and its clause are all committed, so a new owner
//! drains from the start and acts on each clause at the instant it already named. Once the stop
//! ends, a task of its own collects every node's report and commits it beside the ending.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

mod answer;
mod drain;
mod interconnect;
mod request;

use std::collections::BTreeMap;

use nervix_interconnect::domain_stop::StopWaitEntry;
use nervix_models::{ClusterNodeName, CommandExecutionReference, DomainName, DomainState};
use nervix_primitives::{publication::ArcSwap, sync::CancellationToken, task::JoinHandle};

use crate::application::session_service::SessionServiceImpl;

/// What a stopping domain waits for on each node, as its owner's latest round found it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::application) struct StopWaits {
    pub(in crate::application) nodes: BTreeMap<ClusterNodeName, NodeStopWaits>,
}

/// What a stopping domain waits for on one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) enum NodeStopWaits {
    /// Every entry of the node's view: its work in motion and the held work it would cut.
    Observed(Vec<StopWaitEntry>),
    /// The node did not answer the round.
    Unanswered,
}

/// What a request's `CANCEL` found when it acted, kept for its `STOP` to answer with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct CancelledStop {
    pub(in crate::application) domain: DomainName,
    pub(in crate::application) waits: StopWaits,
}

/// The leader's view of the stops it owns: what each stopping domain waits for, and what each
/// cancelled request was waiting for. Both are cold: the owner writes them once a round, and
/// status reads and answers read them.
#[derive(Default)]
pub(in crate::application) struct DomainStops {
    waits: ArcSwap<BTreeMap<DomainName, StopWaits>>,
    cancelled: ArcSwap<BTreeMap<CommandExecutionReference, CancelledStop>>,
}

impl DomainStops {
    /// What `domain`'s stop waits for, while this leader owns it.
    pub(in crate::application) fn waits(&self, domain: &DomainName) -> Option<StopWaits> {
        self.waits.load().get(domain).cloned()
    }

    fn record_waits(&self, domain: &DomainName, waits: StopWaits) {
        self.waits.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.insert(domain.clone(), waits.clone());
            next
        });
    }

    fn forget_waits(&self, domain: &DomainName) {
        if !self.waits.load().contains_key(domain) {
            return;
        }
        self.waits.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove(domain);
            next
        });
    }

    fn record_cancelled(&self, reference: &CommandExecutionReference, cancelled: CancelledStop) {
        self.cancelled.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.insert(reference.clone(), cancelled.clone());
            next
        });
    }

    /// Takes what the cancelled request `reference` was waiting for, once its `STOP` answers.
    fn take_cancelled(&self, reference: &CommandExecutionReference) -> Option<CancelledStop> {
        let cancelled = self.cancelled.load().get(reference).cloned()?;
        self.cancelled.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove(reference);
            next
        });
        Some(cancelled)
    }
}

impl SessionServiceImpl {
    /// The lines a domain's description gives its stop: a stopping domain's requests and what it
    /// waits for, per node and entity, while this node owns it; a stopped domain's cause, how its
    /// stop ended and what its teardown cut.
    pub(in crate::application) fn describe_domain_stop(
        &self,
        domain: &DomainName,
        state: &DomainState,
    ) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(cause) = state.stop_cause().and_then(answer::cause_text) {
            lines.push(cause);
        }
        if let Some(stopping) = state.stopping() {
            lines.push(format!("stop requests: {}", answer::requests_text(stopping)));
            if let Some(waits) = self.inner.domain_stops.waits(domain) {
                lines.push(format!("stop waits for: {}", answer::waits_text(&waits)));
                if let Some(held) = answer::held_text(&waits) {
                    lines.push(format!("stop will cut, as it cannot be saved yet: {held}"));
                }
            }
        }
        // A stop that came before the domain ran any work, a rolled-back start's, resolved no
        // request and cut nothing, so only its cause is described.
        if let Some(ending) = state.stop_ending()
            && !ending.resolved.is_empty()
        {
            lines.push(format!("stop: {}", answer::ending_text(ending, None)));
        }
        lines
    }
}

/// What the leader owes a domain whose record names a stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopWork {
    /// Drain the stop that began at this revision.
    Drain { began_at_revision: u64 },
    /// Collect the report of the stop that ended the domain at this start version.
    Report { start_version: u64 },
}

impl StopWork {
    fn of(state: &DomainState) -> Option<Self> {
        if let Some(stopping) = state.stopping() {
            return Some(Self::Drain {
                began_at_revision: stopping.began_at_revision,
            });
        }
        let ending = state.stop_ending()?;
        if ending.report.is_some() || state.status != nervix_models::DomainStatus::Stopped {
            return None;
        }
        Some(Self::Report {
            start_version: state.start_version,
        })
    }
}

/// One leader task owning one domain's stop work.
struct DomainStopOwner {
    work: StopWork,
    task: JoinHandle<()>,
}

/// Runs on every node; only the leader owns the stops. Each stopping domain has one task that
/// drains it until its stop ends or is called off, and each stopped domain whose report is
/// missing has one that collects it.
pub(in crate::application) async fn run(service: SessionServiceImpl, shutdown: CancellationToken) {
    let observer = service.inner.consensus.observer();
    let mut applied = observer.subscribe_applied();
    let mut topology = observer.subscribe_topology();
    let mut owners: BTreeMap<DomainName, DomainStopOwner> = BTreeMap::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let leads = service
            .inner
            .consensus
            .current_leader_tenure()
            .is_some_and(|tenure| tenure.leader_id() == observer.local_node_id());
        if leads {
            let domains = observer.current_domains().await;
            let mut retired = Vec::new();
            for (domain, owner) in &owners {
                let current = domains.get(domain).and_then(StopWork::of);
                if current != Some(owner.work) || owner.task.is_finished() {
                    retired.push(domain.clone());
                }
            }
            for domain in retired {
                if let Some(owner) = owners.remove(&domain) {
                    owner.task.abort();
                }
                service.inner.domain_stops.forget_waits(&domain);
            }
            for (domain, state) in &domains {
                if owners.contains_key(domain) {
                    continue;
                }
                let Some(work) = StopWork::of(state) else {
                    continue;
                };
                let task = match work {
                    StopWork::Drain { began_at_revision } => nervix_primitives::task::spawn(
                        drain::drain_stopping_domain(
                            service.clone(),
                            domain.clone(),
                            began_at_revision,
                        ),
                    ),
                    StopWork::Report { start_version } => nervix_primitives::task::spawn(
                        drain::collect_stop_report(service.clone(), domain.clone(), start_version),
                    ),
                };
                owners.insert(domain.clone(), DomainStopOwner { work, task });
            }
        } else {
            for (domain, owner) in std::mem::take(&mut owners) {
                owner.task.abort();
                service.inner.domain_stops.forget_waits(&domain);
            }
        }
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            changed = applied.changed() => if changed.is_err() { break; },
            changed = topology.changed() => if !changed { break; },
        }
    }
    for (_, owner) in owners {
        owner.task.abort();
    }
}
