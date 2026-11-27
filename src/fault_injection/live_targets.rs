//! The interconnect live set each node publishes to its transport, as a scenario observes it.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The live set each node last handed its transport, and the waits a scenario runs on
//!   it.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Gossip, application health, the failure detector or relay delivery.

use std::collections::{BTreeMap, BTreeSet};

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

/// The live set each node last published to its transport, by node.
pub(super) type PublishedLiveTargets = BTreeMap<ClusterNodeName, BTreeSet<ClusterNodeName>>;

impl FaultInjection {
    /// Records that `node` handed its transport `live` as the nodes membership keeps live, which
    /// is what ends a delivery or request addressed to a node outside it.
    pub(crate) fn observe_published_live_targets(
        &self,
        node: &ClusterNodeName,
        live: &BTreeSet<ClusterNodeName>,
    ) {
        self.inner
            .published_live_targets
            .send_if_modified(|published| {
                if published.get(node) == Some(live) {
                    return false;
                }
                published.insert(node.clone(), live.clone());
                true
            });
    }

    /// Waits until `observer` has published a live set to its transport that leaves `peer` out,
    /// as it does once its application health gives a stopped peer up.
    pub async fn wait_until_live_targets_omit(
        &self,
        observer: &ClusterNodeName,
        peer: &ClusterNodeName,
    ) {
        let mut published = self.inner.published_live_targets.subscribe();
        published
            .wait_for(|published| {
                published
                    .get(observer)
                    .is_some_and(|live| !live.contains(peer))
            })
            .await
            .assured("the injector keeps the publication sender for as long as it lives");
    }
}
