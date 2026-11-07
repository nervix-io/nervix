//! Receiver attachments to one node's installed clock authorities.
//!
//! Layer: control plane.
//! - **Owns.** Per-domain attachment lifetimes, revision fences and producer notifications.
//! - **Depends on.** Typed clock requests, committed authority specifications and availability.
//! - **Must not know.** Gossip keys, graph preparation, sockets or record processing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns receiver-opened clock delivery lifetimes"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use ahash::HashMap;
use meticulous::ResultExt as _;
use nervix_interconnect::{DomainClockAttachReceipt, DomainClockAttachRequest};
use nervix_models::{ClusterNodeIdentity, ClusterNodeName, DomainName};
use nervix_primitives::{
    publication::ArcSwap,
    sync::{Arc, CancellationToken, StdArc, watch},
};

use super::DomainClockTaskSpec;

#[derive(Clone)]
pub(super) struct DomainClockAttachmentTarget {
    pub identity: ClusterNodeIdentity,
    pub revision: u64,
    pub peer_process_epoch: u64,
    pub lifetime: CancellationToken,
}

struct AttachedClock {
    spec: DomainClockTaskSpec,
    minimum_revision: u64,
    targets: BTreeMap<ClusterNodeName, DomainClockAttachmentTarget>,
    observation: AvailabilityObservationPhase,
}

enum AvailabilityObservationPhase {
    Idle,
    Reading,
    Invalidated,
}

impl AttachedClock {
    fn invalidate_observation(&mut self) -> bool {
        match self.observation {
            AvailabilityObservationPhase::Reading => {
                self.observation = AvailabilityObservationPhase::Invalidated;
                true
            }
            AvailabilityObservationPhase::Idle | AvailabilityObservationPhase::Invalidated => false,
        }
    }

    fn end(&self) {
        for target in self.targets.values() {
            target.lifetime.cancel();
        }
    }
}

#[derive(Clone)]
struct DomainClockAttachmentSlot {
    state: watch::Sender<Option<AttachedClock>>,
}

impl DomainClockAttachmentSlot {
    fn end(&self) {
        self.state.send_modify(|state| {
            if let Some(clock) = state.take() {
                clock.end();
            }
        });
    }
}

struct AttachmentIndex {
    identity: ClusterNodeIdentity,
    domains: ArcSwap<BTreeMap<DomainName, DomainClockAttachmentSlot>>,
}

#[derive(Clone)]
pub(crate) struct DomainClockAttachments {
    index: Arc<AttachmentIndex>,
}

/// The clock controller alone publishes domain slots; a producer retains its exact slot.
pub(crate) struct DomainClockAttachmentInstaller {
    attachments: DomainClockAttachments,
    domains: BTreeMap<DomainName, DomainClockAttachmentSlot>,
}

pub(super) struct DomainClockAttachmentObserver {
    slot: DomainClockAttachmentSlot,
    changes: watch::Receiver<Option<AttachedClock>>,
}

pub(super) enum DomainClockAttachmentObservation {
    Superseded,
    Refresh,
    Current(BTreeMap<ClusterNodeIdentity, DomainClockAttachmentTarget>),
}

impl DomainClockAttachments {
    pub(crate) fn new(identity: ClusterNodeIdentity) -> (Self, DomainClockAttachmentInstaller) {
        let attachments = Self {
            index: Arc::new(AttachmentIndex {
                identity,
                domains: ArcSwap::from_pointee(BTreeMap::new()),
            }),
        };
        let installation = DomainClockAttachmentInstaller {
            attachments: attachments.clone(),
            domains: BTreeMap::new(),
        };
        (attachments, installation)
    }

    pub(crate) fn attach(
        &self,
        authenticated_node: &ClusterNodeName,
        peer_process_epoch: u64,
        request: &DomainClockAttachRequest,
        live: &BTreeSet<ClusterNodeIdentity>,
    ) -> DomainClockAttachReceipt {
        use DomainClockAttachReceipt as Receipt;

        if authenticated_node != request.target.node_id() {
            return Receipt::ForeignTarget;
        }
        if !live.contains(&request.target) {
            return Receipt::UnavailableTarget;
        }
        if request.authority != self.index.identity {
            return Receipt::SupersededAuthority;
        }
        let index = self.index.domains.load();
        let Some(slot) = index.get(&request.domain_id) else {
            return Receipt::SupersededAuthority;
        };
        let mut receipt = Receipt::SupersededAuthority;
        slot.state.send_if_modified(|state| {
            let Some(clock) = state else {
                return false;
            };
            if clock.spec.generation != request.generation
                || clock.spec.authority_revision != request.authority_revision
                || clock.spec.authority != request.authority
            {
                return false;
            }
            let previous = clock.targets.get(request.target.node_id());
            if previous
                .is_some_and(|target| target.identity.incarnation() > request.target.incarnation())
            {
                receipt = Receipt::UnavailableTarget;
                return false;
            }
            if request.installed_revision < clock.minimum_revision
                || previous.is_some_and(|target| {
                    target.identity == request.target
                        && request.installed_revision < target.revision
                })
            {
                receipt = Receipt::StaleRevision;
                return false;
            }
            receipt = Receipt::Accepted;
            if let Some(previous) = previous {
                if previous.identity == request.target
                    && previous.revision == request.installed_revision
                    && previous.peer_process_epoch == peer_process_epoch
                {
                    // A repeated acceptance must invalidate a snapshot being read, even though
                    // it keeps this exact target and lifetime. Further duplicates need no wake.
                    return clock.invalidate_observation();
                }
                // End the preceding lifetime before publishing its replacement.
                previous.lifetime.cancel();
            }
            clock.targets.insert(
                request.target.node_id().clone(),
                DomainClockAttachmentTarget {
                    identity: request.target.clone(),
                    revision: request.installed_revision,
                    peer_process_epoch,
                    lifetime: CancellationToken::new(),
                },
            );
            clock.invalidate_observation();
            true
        });
        receipt
    }

    pub(super) fn observer(
        &self,
        domain: &DomainName,
        spec: &DomainClockTaskSpec,
    ) -> Option<DomainClockAttachmentObserver> {
        let index = self.index.domains.load();
        let slot = index.get(domain)?.clone();
        let changes = slot.state.subscribe();
        if !changes
            .borrow()
            .as_ref()
            .is_some_and(|clock| clock.spec == *spec)
        {
            return None;
        }
        Some(DomainClockAttachmentObserver { slot, changes })
    }
}

impl DomainClockAttachmentInstaller {
    pub(super) fn install(
        &mut self,
        desired: &HashMap<DomainName, DomainClockTaskSpec>,
        revision: u64,
    ) {
        let mut changed = false;
        self.domains.retain(|domain, slot| {
            let keep =
                slot.state.borrow().as_ref().is_some_and(|clock| {
                    desired.get(domain).is_some_and(|spec| *spec == clock.spec)
                });
            if !keep {
                slot.end();
                changed = true;
            }
            keep
        });
        for (domain, spec) in desired {
            if self.domains.contains_key(domain) {
                continue;
            }
            let (state, _) = watch::channel(Some(AttachedClock {
                spec: spec.clone(),
                minimum_revision: revision,
                targets: BTreeMap::new(),
                observation: AvailabilityObservationPhase::Idle,
            }));
            self.domains
                .insert(domain.clone(), DomainClockAttachmentSlot { state });
            changed = true;
        }
        if changed {
            // StdArc is confined to the publication API, which requires the standard type.
            self.attachments
                .index
                .domains
                .store(StdArc::new(self.domains.clone()));
        }
    }

    pub(crate) fn clear(&mut self) {
        for slot in self.domains.values() {
            slot.end();
        }
        self.domains.clear();
        self.attachments
            .index
            .domains
            .store(StdArc::new(BTreeMap::new()));
    }
}

impl Drop for DomainClockAttachmentInstaller {
    fn drop(&mut self) {
        self.clear();
    }
}

impl DomainClockAttachmentObserver {
    pub(super) fn prepare_observation(&mut self) {
        // Register before the caller reads availability and membership. An attachment accepted
        // during those reads must keep its notification and cannot be pruned by their snapshots.
        let changes = &mut self.changes;
        self.slot.state.send_if_modified(|state| {
            // Marking the version without borrowing the value keeps registration and the read
            // phase under one guard, with no additional lock or receiver allocation.
            changes.mark_unchanged();
            if let Some(clock) = state {
                clock.observation = AvailabilityObservationPhase::Reading;
            }
            false
        });
    }

    pub(super) fn observe(
        &mut self,
        live: &BTreeSet<ClusterNodeIdentity>,
        members: &BTreeSet<ClusterNodeName>,
    ) -> DomainClockAttachmentObservation {
        let live_by_name = live
            .iter()
            .map(|identity| (identity.node_id(), identity))
            .collect::<BTreeMap<_, _>>();
        let mut observation = DomainClockAttachmentObservation::Superseded;
        self.slot.state.send_if_modified(|state| {
            let Some(clock) = state else {
                return false;
            };
            clock.observation = AvailabilityObservationPhase::Idle;
            // This check runs under the same value guard as attachment insertion. Its version
            // therefore covers every target that this snapshot could otherwise discard.
            if self
                .changes
                .has_changed()
                .assured("an observer retains its exact slot's sender")
            {
                observation = DomainClockAttachmentObservation::Refresh;
                return false;
            }
            let mut changed = false;
            clock.targets.retain(|node, target| {
                let replaced = live_by_name
                    .get(node)
                    .is_some_and(|identity| *identity != &target.identity);
                let keep = !replaced && (members.contains(node) || live.contains(&target.identity));
                if !keep {
                    target.lifetime.cancel();
                    changed = true;
                }
                keep
            });
            observation = DomainClockAttachmentObservation::Current(
                clock
                    .targets
                    .values()
                    .filter(|target| live.contains(&target.identity))
                    .map(|target| (target.identity.clone(), target.clone()))
                    .collect(),
            );
            changed
        });
        observation
    }

    pub(super) async fn changed(&mut self) {
        // This observer retains the sender of its exact slot, including its terminal state.
        self.changes
            .changed()
            .await
            .assured("an observer retains its exact slot's sender");
    }
}

#[cfg(test)]
mod tests;
