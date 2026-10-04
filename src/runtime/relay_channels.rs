//! Published channel handles belonging to concrete relay branch lifetimes.
//!
//! Layer: data plane.
//! - **Owns.** Single-winner binding, routing generations and exact channel retirement.
//! - **Depends on.** Primitive publication, subscription identities and ordered relay slots.
//! - **Must not know.** Payloads, transport admission, graph planning or NSPL.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "relay producers retain branch channels and observe immutable routing \
                  publications"
    )
)]

use imbl::{GenericHashMap, shared_ptr::DefaultSharedPtr};

use super::*;
use crate::runtime::{AdvertisedSubscriptionInterest, SubscriptionInterestIndex};

type Branches =
    GenericHashMap<Option<BranchKey>, StdArc<RelayBranchChannels>, RandomState, DefaultSharedPtr>;
type Destinations =
    GenericHashMap<RelayOutboundChannel, StdArc<RelayOutboundSlot>, RandomState, DefaultSharedPtr>;

#[derive(Debug)]
pub(super) struct RelayChannels {
    branches: ArcSwap<Branches>,
    closed: CancellationToken,
    routing: StdArc<ArcSwap<CancellationToken>>,
}

/// This handle names a branch incarnation. Its routing publisher can replace channels without
/// allowing an evicted producer to attach to a recreated incarnation of the same branch key.
#[derive(Debug)]
pub(super) struct RelayBranchChannels {
    retired: CancellationToken,
    closed: CancellationToken,
    routing: StdArc<ArcSwap<CancellationToken>>,
    routes: ArcSwap<BranchRoutes>,
}

#[derive(Debug)]
struct BranchRoutes {
    generation: StdArc<CancellationToken>,
    lifetime: CancellationToken,
    ingress: StdArc<RelayOutboundSlot>,
    destinations: ArcSwap<Destinations>,
    subscriptions: ArcSwap<SubscriptionChannels>,
}

#[derive(Debug)]
pub(in crate::runtime) struct SubscriptionChannels {
    snapshot: Option<StdArc<SubscriptionInterestIndex>>,
    interests: Option<BTreeMap<ClusterNodeName, AdvertisedSubscriptionInterest>>,
    lifetime: CancellationToken,
    slots: Destinations,
}

impl Default for RelayChannels {
    fn default() -> Self {
        Self {
            branches: ArcSwap::from_pointee(Branches::default()),
            closed: CancellationToken::new(),
            routing: StdArc::new(ArcSwap::from_pointee(CancellationToken::new())),
        }
    }
}

impl RelayChannels {
    pub(super) fn bind(&self, key: &Option<BranchKey>) -> StdArc<RelayBranchChannels> {
        if self.closed.is_cancelled() {
            return StdArc::new(RelayBranchChannels::new(
                self.routing.clone(),
                self.closed.clone(),
            ));
        }
        let mut current = self.branches.load_full();
        if let Some(branch) = current.get(key) {
            return branch.clone();
        }
        let branch = StdArc::new(RelayBranchChannels::new(
            self.routing.clone(),
            self.closed.clone(),
        ));
        loop {
            if self.closed.is_cancelled() {
                return branch;
            }
            if let Some(winner) = current.get(key) {
                return winner.clone();
            }
            // A persistent map copies only this key's path, independently of sibling branches.
            let mut next = (*current).clone();
            next.insert(key.clone(), branch.clone());
            let observed = self.branches.compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                if self.closed.is_cancelled() {
                    self.retire(key);
                }
                return branch;
            }
            current = StdArc::clone(&observed);
        }
    }

    pub(super) fn retire(&self, key: &Option<BranchKey>) {
        let mut current = self.branches.load_full();
        let Some(branch) = current.get(key).cloned() else {
            return;
        };
        branch.retire();
        loop {
            let Some(published) = current.get(key) else {
                return;
            };
            if !StdArc::ptr_eq(published, &branch) {
                return;
            }
            let mut next = (*current).clone();
            next.remove(key);
            let observed = self.branches.compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return;
            }
            current = StdArc::clone(&observed);
        }
    }

    /// Lifecycle writers are serialized by schedule application and its dispatch fence. Cancelling
    /// the parent also fences a first-use candidate created concurrently from this generation.
    pub(super) fn replace_owner(&self) {
        if self.closed.is_cancelled() {
            return;
        }
        self.routing.load().cancel();
        self.routing.store(StdArc::new(CancellationToken::new()));
    }

    pub(super) fn replace_destinations(&self) {
        self.replace_owner();
    }

    pub(super) fn retire_all(&self) {
        self.closed.cancel();
        self.routing.load().cancel();
        for branch in self.branches.load().values() {
            branch.retire();
        }
        self.branches.store(StdArc::new(Branches::default()));
    }
}

impl Drop for RelayChannels {
    fn drop(&mut self) {
        self.retire_all();
    }
}

impl BranchRoutes {
    fn new(generation: StdArc<CancellationToken>) -> Self {
        let lifetime = generation.child_token();
        Self {
            ingress: StdArc::new(RelayOutboundSlot::with_cancellation(lifetime.child_token())),
            destinations: ArcSwap::from_pointee(Destinations::default()),
            subscriptions: ArcSwap::from_pointee(SubscriptionChannels {
                snapshot: None,
                interests: None,
                lifetime: lifetime.child_token(),
                slots: Destinations::default(),
            }),
            generation,
            lifetime,
        }
    }
}

impl RelayBranchChannels {
    fn new(routing: StdArc<ArcSwap<CancellationToken>>, closed: CancellationToken) -> Self {
        let routes = ArcSwap::from_pointee(BranchRoutes::new(routing.load_full()));
        Self {
            retired: CancellationToken::new(),
            closed,
            routing,
            routes,
        }
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retired.is_cancelled()
            || self.closed.is_cancelled()
            || self.routing.load().is_cancelled()
    }

    fn routes(&self) -> StdArc<BranchRoutes> {
        let mut current = self.routes.load_full();
        loop {
            if self.is_retired() {
                current.lifetime.cancel();
                return current;
            }
            let generation = self.routing.load_full();
            if StdArc::ptr_eq(&current.generation, &generation) {
                return current;
            }
            let next = StdArc::new(BranchRoutes::new(generation));
            let observed = self.routes.compare_and_swap(&current, next.clone());
            if StdArc::ptr_eq(&current, &observed) {
                // Retirement may have loaded the preceding value before this CAS won.
                if self.is_retired() {
                    next.lifetime.cancel();
                }
                return next;
            }
            current = StdArc::clone(&observed);
        }
    }

    pub(super) fn ingress(&self) -> StdArc<RelayOutboundSlot> {
        self.routes().ingress.clone()
    }

    pub(super) fn outbound(&self, channel: RelayOutboundChannel) -> StdArc<RelayOutboundSlot> {
        let routes = self.routes();
        let mut current = routes.destinations.load_full();
        if let Some(slot) = current.get(&channel) {
            return slot.clone();
        }
        let slot = StdArc::new(RelayOutboundSlot::with_cancellation(
            routes.lifetime.child_token(),
        ));
        loop {
            if let Some(winner) = current.get(&channel) {
                return winner.clone();
            }
            if routes.lifetime.is_cancelled() {
                return slot;
            }
            let mut next = (*current).clone();
            next.insert(channel.clone(), slot.clone());
            let observed = routes
                .destinations
                .compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return slot;
            }
            current = StdArc::clone(&observed);
        }
    }

    /// A changed gossip snapshot is a cold publication event. Equal advertisements preserve slots;
    /// withdrawal or incarnation/version replacement cancels the complete subscription generation.
    /// The selected table contains only live advertised peers, bounding retained churn state.
    pub(super) fn subscriptions(
        &self,
        snapshot: StdArc<SubscriptionInterestIndex>,
        domain: &DomainName,
        relay: &RelayName,
    ) -> StdArc<SubscriptionChannels> {
        let routes = self.routes();
        let mut current = routes.subscriptions.load_full();
        loop {
            if current
                .snapshot
                .as_ref()
                .is_some_and(|selected| StdArc::ptr_eq(selected, &snapshot))
            {
                return current;
            }
            let interests = snapshot.nodes(domain.as_str(), relay.as_str());
            let same = current.interests.as_ref() == interests;
            let lifetime = if same {
                current.lifetime.clone()
            } else {
                current.lifetime.cancel();
                routes.lifetime.child_token()
            };
            let slots = if same {
                current.slots.clone()
            } else {
                interests
                    .into_iter()
                    .flat_map(|nodes| nodes.keys())
                    .map(|node_id| {
                        (
                            RelayOutboundChannel {
                                node_id: node_id.clone(),
                                relay: relay.clone(),
                                kind: RelayPayloadKind::SubscriptionFanout,
                            },
                            StdArc::new(RelayOutboundSlot::with_cancellation(
                                lifetime.child_token(),
                            )),
                        )
                    })
                    .collect()
            };
            let next = StdArc::new(SubscriptionChannels {
                snapshot: Some(snapshot.clone()),
                interests: interests.cloned(),
                lifetime,
                slots,
            });
            let observed = routes
                .subscriptions
                .compare_and_swap(&current, next.clone());
            if StdArc::ptr_eq(&current, &observed) {
                return next;
            }
            current = StdArc::clone(&observed);
        }
    }

    fn retire(&self) {
        self.retired.cancel();
        self.routes.load().lifetime.cancel();
    }
}

impl SubscriptionChannels {
    pub(in crate::runtime) fn slot(
        &self,
        node_id: &ClusterNodeName,
        relay: &RelayName,
    ) -> Option<StdArc<RelayOutboundSlot>> {
        self.slots
            .get(&RelayOutboundChannel {
                node_id: node_id.clone(),
                relay: relay.clone(),
                kind: RelayPayloadKind::SubscriptionFanout,
            })
            .cloned()
    }
}

#[cfg(test)]
#[path = "relay_channels_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "relay_channels_shuttle_tests.rs"]
mod shuttle_tests;
