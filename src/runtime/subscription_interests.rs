//! Immutable subscription facts selected by relay delivery.
//!
//! Layer: data plane.
//! - **Owns.** The live subscriber index, incarnation/version selection and immutable reads.
//! - **Depends on.** Vocabulary identities and ordinary collections.
//! - **Must not know.** Gossip drivers, cluster coordination, NSPL or session framing.

use std::collections::BTreeMap;

use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};

/// The interested node incarnations for every domain and relay advertised by live gossip state.
///
/// Strings are retained only in this cold-path snapshot. A relay owner looks them up through
/// borrowed `str` keys, then iterates the node map without formatting a gossip key or allocating.
#[derive(Debug, Default)]
pub(crate) struct SubscriptionInterestIndex {
    domains: BTreeMap<
        String,
        BTreeMap<String, BTreeMap<ClusterNodeName, AdvertisedSubscriptionInterest>>,
    >,
}

/// One live advertisement, fenced by both the node incarnation and its interest key's version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AdvertisedSubscriptionInterest {
    pub(crate) incarnation: ClusterNodeIncarnation,
    pub(crate) version: u64,
}

impl SubscriptionInterestIndex {
    pub(crate) fn record(
        &mut self,
        domain: &str,
        relay: &str,
        identity: &ClusterNodeIdentity,
        version: u64,
    ) {
        let advertisement = AdvertisedSubscriptionInterest {
            incarnation: identity.incarnation(),
            version,
        };
        let nodes = self
            .domains
            .entry(domain.to_string())
            .or_default()
            .entry(relay.to_string())
            .or_default();
        match nodes.entry(identity.node_id().clone()) {
            std::collections::btree_map::Entry::Occupied(mut current) => {
                if advertisement > *current.get() {
                    current.insert(advertisement);
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(advertisement);
            }
        }
    }

    pub(crate) fn nodes(
        &self,
        domain: &str,
        relay: &str,
    ) -> Option<&BTreeMap<ClusterNodeName, AdvertisedSubscriptionInterest>> {
        let relays = self.domains.get(domain)?;
        relays.get(relay)
    }

    pub(crate) fn contains(
        &self,
        subscriber: &ClusterNodeIdentity,
        domain: &str,
        relay: &str,
        minimum_version: u64,
    ) -> bool {
        let Some(nodes) = self.nodes(domain, relay) else {
            return false;
        };
        let Some(advertisement) = nodes.get(subscriber.node_id()) else {
            return false;
        };
        advertisement.incarnation == subscriber.incarnation()
            && advertisement.version >= minimum_version
    }
}
